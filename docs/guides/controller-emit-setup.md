# Optional Controller event precision layer

The Herdr plugin's primary orchestration view requires no hook registration or
`emit` wiring. Open its pane and herdr-top reads Claude Code and Codex provider
session logs directly, synthesizing the session's agent tree, headless workers,
run lifecycle, live activity, and token telemetry from those artifacts.

Controller events are an optional precision layer over that view. Provider
hooks add explicit lifecycle transitions, Controller-authored subjects, and
dispatch edges that do not depend on session-ID evidence. Claude Code hooks can
also report task-run creation and completion. Manual `emit` events can add
explicit dependencies. Hooks and manual events are not authoritative for
physical workspace, tab, pane, or execution topology; complete Herdr snapshots
remain the authority for those entities.

The hook integration does not produce dependency edges. Neither provider's hook
surface can derive semantic dependencies between runs. Add those relationships
explicitly as described in [Add dependency edges manually](#add-dependency-edges-manually).

Claude Agent-tool children carry lineage through `.meta.json` sidecars that
name the parent and agent type. The other admitted positions are actual Bash
tool-use commands `codex exec resume <uuid>` and `claude --resume <uuid>`,
leading `CLAUDE_CONFIG_DIR=` assignments, and Codex's typed
`sub_agent_activity.agent_thread_id` child reference. Resume UUIDs must exactly
match discovered artifacts. Quoted reports, printed command lookalikes,
tool-result bodies, bare `codex exec` or `claude -p` spawns, and spawn output are
not evidence. Measurements on the reference development machine found the
child ID in only a small minority of bare spawn command lines; because a fresh
child normally assigns its ID after launch, those spawns are intentionally not
used for lineage. Linking wrapper children whose IDs surface only in spawn
output is deferred; herdr-top does not guess from timing, neighboring panes, or
shared paths.

## Install the standalone CLI

The managed Herdr plugin is sufficient for zero-configuration live monitoring.
Hook and other Controller-event users must also install the standalone
`herdr-top` binary from the same release, verify its checksum, and place it on
`PATH`.

Run these commands from an interactive shell to verify the binary and its
diagnostics:

```sh
herdr-top --version
herdr-top emit --help
herdr-top doctor
herdr-top doctor --json
```

`herdr-top emit --help` and `herdr-top --version` are interactive checks only.
They must not appear in registered hook commands, as explained in
[Keep hook standard output empty](#keep-hook-standard-output-empty).

## Understand the event mapping

The `--from-hook` provider values are exactly `claude-code` and `codex`. Both
providers use the same case-sensitive PascalCase event names.

| Hook event | Provider | Emitted Controller events | Run affected |
| --- | --- | --- | --- |
| `SessionStart` | both | `task_started` with binding identity | session run |
| `SubagentStart` | both | `dispatch`, then `task_started`; label is the agent type | subagent run |
| `SubagentStop` | both | `complete`; for Claude Code only, an explicitly present empty `agent_type` string maps to nothing | subagent run |
| `TaskCreated` | Claude Code only | `dispatch`, then `progress`; label is the task subject | task run |
| `TaskCompleted` | Claude Code only | `complete` | task run |
| `SessionEnd` | both | `session_ended` native lifecycle `Done` | session run |
| any other event | both | nothing | none |

Codex has no `TaskCreated` or `TaskCompleted` equivalent, so task runs appear
only for Claude Code. Codex does report session and subagent runs with the same
`SessionStart`, `SubagentStart`, and `SubagentStop` spellings as Claude Code.

In manual mode, `--event-id`, `--emitted-at-ms`, `--source`, `--event-type`, and
`--task-run-id` are required. Adapter mode derives those fields and any mapped
relationship or metadata fields from the hook payload. Do not combine
`--from-hook` with any manual envelope flag: `--event-id`, `--emitted-at-ms`,
`--source`, `--event-type`, `--task-run-id`, `--parent-task-run-id`,
`--depends-on-id`, `--label`, `--reason`, `--progress`, `--provider`,
`--native-session-id`, and `--terminal-id` all conflict with it.

## Register Claude Code hooks

Claude Code hooks live in `~/.claude/settings.json`. Append each entry shown
below to the corresponding event's existing array in the operator's live file.
Create an event key only when it is absent. This is a merge, never a replacement
of the existing `hooks` object, an event array, or an existing handler. Operator
files routinely contain other handlers, such as context-mode and
permission-guard hooks, and those handlers must remain present.

```json
{
  "hooks": {
    "SessionStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "SessionEnd": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "SubagentStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "SubagentStop": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "TaskCreated": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "TaskCompleted": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}]
  }
}
```

## Register Codex hooks

Codex hooks live in `~/.codex/hooks.json`. As with Claude Code, append each new
entry to the corresponding event's existing array. Create a missing event key,
but do not replace any existing event array or handler. A Codex file may already
contain Herdr's own integration hooks and guard hooks; preserve all of them.

```json
{
  "hooks": {
    "SessionStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook codex"}]}],
    "SessionEnd": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook codex"}]}],
    "SubagentStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook codex"}]}],
    "SubagentStop": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook codex"}]}]
  }
}
```

After saving the registration, complete Codex's hook-trust acceptance for the
updated hook file when Codex asks. Codex does not run the newly registered
commands until this post-registration trust step has been accepted.

Hooks run in parallel and coexist with Herdr's own integration hooks. Appending
the entries above preserves that coexistence; it does not disable, serialize, or
otherwise disturb existing handlers.

## Merge and verify an operator file

Apply this procedure separately to each file being changed:

1. Back up the complete file before editing it. The following examples use a
   stable backup name so the later verification commands can compare the files.
   If that name already exists, choose a new suffix instead of overwriting it.

   ```sh
   test ! -e "$HOME/.claude/settings.json.before-herdr-top" \
     && cp "$HOME/.claude/settings.json" "$HOME/.claude/settings.json.before-herdr-top"
   test ! -e "$HOME/.codex/hooks.json.before-herdr-top" \
     && cp "$HOME/.codex/hooks.json" "$HOME/.codex/hooks.json.before-herdr-top"
   ```

2. Append the new entry object to each event's existing array. Create the event
   key and its array only when the key is absent. Do not paste either fragment
   over the complete file or over its complete `hooks` object.

3. Verify that the edited file still parses. Run the applicable command, or run
   both after changing both providers.

   ```sh
   jq empty "$HOME/.claude/settings.json"
   jq empty "$HOME/.codex/hooks.json"
   ```

4. List the registered command strings per event and inspect the output for both
   the new commands and every pre-existing command.

   ```sh
   jq -r '
     (.hooks // {}) | to_entries[]
     | .key as $event
     | .value[]?.hooks[]?
     | select(.type == "command")
     | [$event, .command]
     | @tsv
   ' "$HOME/.claude/settings.json"

   jq -r '
     (.hooks // {}) | to_entries[]
     | .key as $event
     | .value[]?.hooks[]?
     | select(.type == "command")
     | [$event, .command]
     | @tsv
   ' "$HOME/.codex/hooks.json"
   ```

5. Confirm that every pre-existing event-array entry is still present, in its
   original order, at the start of the edited array. Each command prints `true`
   and exits 0 only when the append-only check passes.

   ```sh
   jq -e --slurpfile before "$HOME/.claude/settings.json.before-herdr-top" '
     (.hooks // {}) as $after
     | ($before[0].hooks // {})
     | to_entries
     | all(.[];
         . as $event
         | ($after[$event.key] // [])[0:($event.value | length)] == $event.value)
   ' "$HOME/.claude/settings.json"

   jq -e --slurpfile before "$HOME/.codex/hooks.json.before-herdr-top" '
     (.hooks // {}) as $after
     | ($before[0].hooks // {})
     | to_entries
     | all(.[];
         . as $event
         | ($after[$event.key] // [])[0:($event.value | length)] == $event.value)
   ' "$HOME/.codex/hooks.json"
   ```

6. Compare the handler count for every event in the backup and edited file. The
   last two columns are the before and after counts. An unchanged event has equal
   counts; each event registered above has one additional top-level entry.

   ```sh
   jq -nr \
     --slurpfile before "$HOME/.claude/settings.json.before-herdr-top" \
     --slurpfile after "$HOME/.claude/settings.json" '
     (($before[0].hooks // {}) + ($after[0].hooks // {}))
     | keys[] as $event
     | [$event,
        (($before[0].hooks[$event] // []) | length),
        (($after[0].hooks[$event] // []) | length)]
     | @tsv
   '

   jq -nr \
     --slurpfile before "$HOME/.codex/hooks.json.before-herdr-top" \
     --slurpfile after "$HOME/.codex/hooks.json" '
     (($before[0].hooks // {}) + ($after[0].hooks // {}))
     | keys[] as $event
     | [$event,
        (($before[0].hooks[$event] // []) | length),
        (($after[0].hooks[$event] // []) | length)]
     | @tsv
   '
   ```

Complete Codex hook-trust acceptance only after these registration and merge
checks pass.

## Keep hook standard output empty

Never add `--help`, `-h`, `--version`, or `-V` to a registered hook command.
Clap writes help and version text to standard output. Claude Code and Codex both
parse hook standard output as structured hook output; Codex additionally checks
that output against a closed schema and marks a hook invalid when it encounters
unrecognized JSON.

The adapter deliberately writes nothing to standard output. Accepted responses,
warnings, and all other adapter diagnostics go to standard error. A registered
command must therefore never be a command that prints help, version text, or
anything else to standard output. Use `herdr-top --version` only from an
interactive shell.

The reference registrations do not use `--strict`. Malformed payloads, unmapped
events, unavailable sessions, and delivery failures are adapter outcomes that
exit 0 in this mode. Nothing the adapter does can make one of the reference hook
commands exit non-zero, so agent work is not blocked by monitoring delivery.

## Understand delivery behavior

Within one hook invocation, mapped envelopes are delivered strictly in order.
Delivery stops at the first failure. In particular, the adapter never delivers a
child's `task_started` after the preceding `dispatch` has failed, because that
would create a run with no recorded task relationships.

If an earlier hook is lost, a later terminal event can still create a
forward-referenced terminal run. Herdr Top flags that degraded outcome in
diagnostics; it is not database or execution-tree corruption. Inspect hook
standard error to find the earlier delivery failure.

Claude Code also emits `SubagentStop` hooks whose payload carries an explicitly
present but empty `agent_type` string (`""`). The observed payloads of that
shape arrived with no preceding `SubagentStart`, so mapping them to `complete`
would create diagnostic-flagged terminal forward-reference runs. Attributing
that shape to provider-internal agents is an inference; the adapter's actual
discriminator is structural. For the `claude-code` provider only, a
`SubagentStop` whose `agent_type` is present and equal to `""` maps to nothing:
it is not delivered, writes nothing to standard output or standard error, and
cannot fail `--strict`. The adapter never inspects transcript paths to make
that decision. A `SubagentStop` with an absent `agent_type`, a JSON `null`, or
a non-empty string keeps the `complete` mapping and the lost-start recovery
described here. Codex stops are unchanged, including one with an explicit
empty type.

Because hooks run in parallel, a terminal hook can occasionally deliver before
its start hook. When the delayed start later delivers `task_started`, the
controller rejects it with reason `stale_event`. The adapter treats that reason
as benign: it logs it to standard error, continues delivery, and does not count
it as a delivery failure or a `--strict` failure.

Semantic Task state, provider-native lifecycle, execution state, pane status,
and graph relationships are separate evidence axes. A normal `SessionEnd`
records native lifecycle `Done`; explicit provider abort and failure facts
record `Cancelled` and `Error`, and disappearance without stronger evidence
records `Unknown`. None of those facts writes semantic completion,
cancellation, or failure. Codex turn completion is runtime Idle only.

On the separate Herdr event surface, the collector accepts exactly
`pane.agent_status_changed` and legacy `pane_agent_status_changed` as aliases
for pane status. Both update the same gauge; other spellings are ignored.

`session_ended` applies only to an existing run with a matching non-empty
provider/native-session binding. An unknown or unbound session is a diagnostic
no-op and creates no forward-reference placeholder. It also does not dismiss
the run: the terminal-looking lifecycle row remains default-visible for one
hour, while operator dismissal with `c` stays a separate explicit visibility
action.

A matching later `task_started`, live execution, or provider liveness fact
clears native lifecycle evidence on the same Task Run and preserves its run ID
and display ordinal. It never reopens a semantic terminal Task state. Lifecycle
ordering compares trustworthy source time, then collector observation time,
then a stable source or event identity. Repeating the same watermark and status
is idempotent; an older delayed fact cannot re-close a newer resume after
delivery delay or restart.

The superseded dismissal behavior and the current lifecycle decision are
documented in
[Session end auto-dismiss](../adr/2026-08-22-session-end-auto-dismiss.md).

When no Herdr session can be resolved, the adapter delivers nothing, warns on
standard error, and exits 0. This is the expected outcome for an agent session
started outside a managed pane, not an integration error that needs repair.

## Test a hook without an agent

`herdr-top emit --from-hook <provider>` reads standard input to end-of-file like
a filter. Running it directly with no pipe or redirected input leaves it waiting
for EOF, which can look like a hang. Pipe a complete JSON object instead.

This malformed-payload probe exercises the adapter's safe no-op path:

```sh
echo '{}' | herdr-top emit --from-hook claude-code
```

It prints nothing to standard output, warns about the invalid payload on
standard error, and exits 0.

This realistic payload exercises the `SessionStart` mapping while using a
throwaway Herdr session name:

```sh
printf '%s\n' '{"hook_event_name":"SessionStart","session_id":"session-123"}' \
  | herdr-top --session herdr-top-doc-probe emit --from-hook claude-code
```

It also prints nothing to standard output and exits 0. If no controller owns the
throwaway session, it reports an unavailable delivery on standard error. If a
controller does own that session, the accepted delivery response is still sent
only to standard error.

## Add dependency edges manually

Hook-created execution edges answer "which session dispatched this run?" A
`depends_on` edge answers a different semantic question: "which prerequisite
must this run depend on?" No hook event provides enough information to infer
that relationship.

The following complete manual event states that Claude Code task `task-2`
depends on task `task-1`:

```sh
herdr-top --session herdr-top-doc-probe emit \
  --event-id manual-dependency-001 \
  --emitted-at-ms 1787184000000 \
  --source operator:manual \
  --event-type depends_on \
  --task-run-id hook:claude-code:session-123:task:task-2 \
  --depends-on-id hook:claude-code:session-123:task:task-1
```

Manual mode requires `--event-id`, `--emitted-at-ms`, `--source`,
`--event-type`, and `--task-run-id`. The controller requires
`--depends-on-id` for `depends_on` and forbids `--parent-task-run-id` for that
event type. The example includes every required flag, omits the forbidden
parent flag, and relies on the default schema version 1. Replace the session,
event identifier, timestamp, and run identifiers with current values.

Hook-created run identifiers are deterministic. Given a provider hook payload:

- the session run is `hook:<provider>:<native-session-id>`;
- a subagent run appends `:agent:<agent-id>`;
- a Claude Code task run appends `:task:<task-id>`.

Obtain `session_id` and the two `task_id` or `agent_id` values from the relevant
provider hook payloads, then construct the two identifiers using these rules.
In a `depends_on` event, `--task-run-id` is the dependent subject and
`--depends-on-id` is its prerequisite.

## Understand displayed labels

Subagent runs display the hook's agent type. Claude Code task runs display the
task subject in the detail pane's activity lines.

The task subject is agent-authored content, so its provenance boundary is
deliberately narrow. The task's one-line subject is the only agent-generated
content permitted in a Controller label. It passes the existing label
sanitization: a 256-byte cap, control-character escaping, and UTF-8-safe
truncation. Prompts, responses, descriptions, tool inputs, tool results, and all
other agent-generated content are never forwarded as labels.

## Diagnose problems

Run the human-readable or fixed-schema JSON diagnostic report:

```sh
herdr-top doctor
herdr-top doctor --json
```

`doctor` checks the Herdr socket; the resolved session key and whether it came
from a flag, environment variable, or the `default` managed-pane rule;
breadcrumb and `session-name.txt` validity; the runtime sentinel and current
Controller-socket availability; socket-path length; the state lock and database
schema; provider discovery; official Herdr integration versions; plugin and CLI
compatibility; native-session coverage; provider-log lane health; and log
locations. It queries versions from the relevant binary or server instead of
inferring them from installation paths, and it does not print prompts or
responses.

`coverage.native_sessions` consumes the lane-wide
`pane_sessions_with_artifacts` count as a shared budget for identifier-kind
panes in snapshot order. Even the aggregate `covered` and `uncovered` counts
are approximate and can be optimistic: path-kind panes contribute budget units
without consuming one, and retained historical executions can fund a current
pane. Treat the counts as a hint, not a guarantee. The `by_provider` placement
is not proof of artifact ownership, and a pane from one provider can consume a
budget unit produced by the other provider.

The log-lane checks are:

| Check | Outcomes |
| --- | --- |
| `log_lane.readable` | `warning` / `log_lane_roots_unreadable` when an existing root cannot be read; `ok` / `log_lane_roots_readable` when one exists and is readable; `not_applicable` / `log_lane_roots_absent` when none exists. |
| `log_lane.coverage` | `warning` / `log_lane_targets_rejected` when any targets were rejected, taking precedence over coverage; `not_applicable` / `log_lane_coverage_empty` with no pane sessions; `warning` / `log_lane_coverage_partial` when some pane session lacks an artifact; otherwise `ok` / `log_lane_coverage_complete`. |
| `log_lane.freshness` | `ok` / `log_lane_fresh` when the latest watcher observation is at most 120000 ms old; `warning` / `log_lane_stale` beyond that; `warning` / `log_lane_unobserved` before any watcher observation. |

Coverage and freshness use `warning` / `log_lane_runtime_unavailable` when
runtime diagnostics are unavailable. Freshness comes from the watcher's own
observation timestamp, not file modification times. Coverage includes pane
session totals, counts with and without artifacts, and rejected-target counts.
The `rejected_targets` counter is cumulative for the process lifetime and is
never reset. After any rejection, `log_lane.coverage` remains `warning` until
herdr-top restarts, even if the cause is fixed; because rejection takes
precedence, that warning also suppresses the `log_lane_coverage_partial` and
`log_lane_coverage_complete` codes. The check's `observed` payload still carries
the raw `pane_sessions_total`, `pane_sessions_with_artifacts`,
`pane_sessions_without_artifacts`, and `rejected_targets` values.

Use these checks for common symptoms:

| Symptom | Check |
| --- | --- |
| Nothing appears in the TUI | Inspect `log_lane.readable`, `log_lane.coverage`, and `log_lane.freshness`, then confirm session resolution and Herdr reachability. Hook setup is not required for the primary view. |
| The tree appears but optional Controller detail does not | Confirm that the same-release standalone binary is on the hook process's `PATH`, recheck the append-only registration, complete Codex hook trust, and inspect Controller-socket availability. Outside a managed pane, a clean no-delivery result is expected. |
| Runs have no recorded task relationships | Open Selected Detail and inspect `dispatch_parent`, `prerequisites`, `dependents`, and `task_relationships`. Then inspect hook standard error for a failed `dispatch`, confirm that the relevant start hook remains registered, and review [Understand delivery behavior](#understand-delivery-behavior). A later terminal event can intentionally create a diagnostic-flagged forward reference. |
| An admitted inner Codex run appears under `Unattached` | Supply lineage through a `.meta.json` subagent record, a Codex `sub_agent_activity.agent_thread_id` reference, or an actual `codex exec resume <uuid>` / `claude --resume <uuid>` Bash invocation; otherwise use explicit Controller dispatch events. Merely echoing the ID into the transcript is not evidence. |
| A subagent run has no label | Confirm that `SubagentStart` is registered and that its payload contains `agent_type`; the label comes only from that structural field. |
| A manual hook test seems to hang | The adapter is waiting for standard-input EOF. Use a piped probe from [Test a hook without an agent](#test-a-hook-without-an-agent). |
