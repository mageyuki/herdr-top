# Controller event hook setup

Herdr Top can receive Claude Code and Codex hook events and add their execution
structure to the live view. The integration reports session runs, subagent runs,
and their lifecycle transitions. Claude Code also reports task-run creation and
completion. The resulting `dispatch` edges show which session launched each
subagent or task.

The hook integration does not produce dependency edges. Neither provider's hook
surface can derive semantic dependencies between runs. Add those relationships
explicitly as described in [Add dependency edges manually](#add-dependency-edges-manually).

## Install the standalone CLI

The managed Herdr plugin is sufficient for live monitoring. Hook and other
Controller-event users must also install the standalone `herdr-top` binary from
the same release, verify its checksum, and place it on `PATH`.

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
| `SubagentStop` | both | `complete` | subagent run |
| `TaskCreated` | Claude Code only | `dispatch`, then `progress`; label is the task subject | task run |
| `TaskCompleted` | Claude Code only | `complete` | task run |
| `SessionEnd` | both | nothing | none |
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
would create a permanently unlinked run.

If an earlier hook is lost, a later terminal event can still create a
forward-referenced terminal run. Herdr Top flags that degraded outcome in
diagnostics; it is not database or execution-tree corruption. Inspect hook
standard error to find the earlier delivery failure.

Because hooks run in parallel, a terminal hook can occasionally deliver before
its start hook. When the delayed start later delivers `task_started`, the
controller rejects it with reason `stale_event`. The adapter treats that reason
as benign: it logs it to standard error, continues delivery, and does not count
it as a delivery failure or a `--strict` failure.

`SessionEnd` is deliberately mapped to nothing. The adapter exits 0 silently for
that event. Sessions routinely resume, while a resumed `task_started` would be
rejected as `stale_event` if the session run had already been made terminal.
Session-run liveness instead follows observed executions. Do not register
`SessionEnd`; it delivers nothing, and its silence is not a failure.

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
compatibility; native-session coverage; and log locations. It queries versions
from the relevant binary or server instead of inferring them from installation
paths, and it does not print prompts or responses.

Use these checks for common symptoms:

| Symptom | Check |
| --- | --- |
| Nothing appears in the TUI | Confirm that the same-release standalone binary is on the hook process's `PATH`, recheck the append-only registration, complete Codex hook trust, and inspect `doctor` session resolution and Controller-socket availability. Outside a managed pane, a clean no-delivery result is expected. |
| Runs appear but stay unlinked | Inspect hook standard error for a failed `dispatch`, confirm that the relevant start hook remains registered, and review [Understand delivery behavior](#understand-delivery-behavior). A later terminal event can intentionally create a diagnostic-flagged forward reference. |
| A subagent run has no label | Confirm that `SubagentStart` is registered and that its payload contains `agent_type`; the label comes only from that structural field. |
| A manual hook test seems to hang | The adapter is waiting for standard-input EOF. Use a piped probe from [Test a hook without an agent](#test-a-hook-without-an-agent). |
