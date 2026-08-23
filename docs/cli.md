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
hook event. See [Controller event hook setup](guides/controller-emit-setup.md)
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
native-session coverage, and logs. It exits with status 1 when any check has
`error`; warnings alone do not produce a failing exit status.

Individual checks use the lowercase statuses `ok`, `warning`, `error`, and
`not_applicable`. The last status is commonly emitted when a check does not
apply, such as the breadcrumb path check for a non-plugin install.
`overall_status` appears in the human-readable header line and as a JSON field.
It is the worst-of severity across the checks and has only `ok`, `warning`, or
`error`; there is no `not_applicable` overall status.

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
