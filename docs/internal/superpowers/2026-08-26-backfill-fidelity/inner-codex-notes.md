# Inner child lineage evidence from real wrapper pairs

This note tests the typed lineage grammar in
`docs/internal/superpowers/2026-08-26-backfill-fidelity/spec.md` against two
real parent/child pairs. The relevant closed-whitelist positions are a child
identity in a recognized spawn or resume command (position 2), a Codex
`sub_agent_activity.agent_thread_id` reference (position 3), or a named field
on a tool-use-correlated result record (position 4). A UUID that occurs only in
message or tool-result text is not typed evidence under G2b.

All timestamps below are UTC. Personal directory prefixes have been redacted
from excerpts, but session UUIDs, artifact basenames, field names, command
options, and tool-use IDs are preserved. Whitespace was added to JSON excerpts
for readability; no field nesting was changed.

## Pair 1: `codex-implementer` wrapper and inner Codex rollout

### Artifacts examined and pairing

The parent is the Claude sub-agent transcript
`27376f99-8093-4a65-a87f-4e4158f4db69/subagents/agent-afbb3cc03cf3fd4f8.jsonl`
in the user's Claude configuration projects directory. Its first record names
`agentId` `afbb3cc03cf3fd4f8` and describes an implementation task in the
`herdr-top-wt-resilience` linked worktree.

The child is
`rollout-2026-08-26T06-51-59-01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a.jsonl`
in the Codex sessions directory's `2026/08/26` shard. This is a positive
pairing, based on all of the following independent agreements:

- The parent launched `codex exec` at `2026-08-25T21:51:56.272Z`; the child's
  metadata timestamps its start at `2026-08-25T21:51:59.997Z`.
- Both artifacts name the same `herdr-top-wt-resilience` worktree and the same
  starting Git commit, `126c388fe513ac73fae5ab237661b8c6b89fa238`.
- The input prompt in the rollout matches the prompt file passed to the parent
  command, including its opening dispatched-implementer wording and task
  constraints.
- The parent command requests `model_reasoning_effort="xhigh"`. The captured
  Codex prelude reports version `0.149.1`, model `gpt-5.6-sol`, and reasoning
  effort `xhigh`, while naming the child UUID below.
- The `-o` destination resolved to `codex-final-PTs4ZF.txt`; the same parent
  result that reports that basename also contains the matching Codex prelude.

### Identity carried by the child

The filename UUID is `01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a`. The first rollout
record carries the same value twice, at `root.payload.session_id` and
`root.payload.id`:

```json
{
  "timestamp": "2026-08-25T21:52:00.301Z", "type": "session_meta",
  "payload": {
    "session_id": "01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a",
    "id": "01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a",
    "timestamp": "2026-08-25T21:51:59.997Z", "cwd": "[personal path redacted]",
    "originator": "codex_exec", "source": "exec", "cli_version": "0.149.1"
  }
}
```

The observed `source` is the string `exec`, not a typed Codex sub-agent source
that names the Claude wrapper.

### Lineage position

Position 2 contains the spawn command but not the child identity. The exact
nesting is `root(type="assistant").message.content[*](type="tool_use",
name="Bash").input.command`:

```json
{
  "type": "assistant", "timestamp": "2026-08-25T21:51:56.272Z",
  "sessionId": "27376f99-8093-4a65-a87f-4e4158f4db69",
  "agentId": "afbb3cc03cf3fd4f8", "message": {"content": [{
    "type": "tool_use", "id": "toolu_01H6N5tytctX2mJE2EEoMnJE", "name": "Bash",
    "input": {"command": "SP=[personal path redacted]\nOUT=$(mktemp \"$SP/codex-final-XXXXXX.txt\")\n...\nnohup codex exec -s workspace-write -C [personal path redacted] -c 'model_reasoning_effort=\"xhigh\"' -o \"$OUT\" - < \"$SP/prompt.txt\" > \"$SP/codex-stdout.log\" 2>&1 &\n..."}
  }]}
}
```

A fresh `codex exec` assigns its identity after launch, so neither this command
nor its correlated background-launch result carries
`01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a`. The launch result has typed
`root.toolUseResult.backgroundTaskId`, but that value is a Claude background
task ID, not a Codex session ID.

Position 3 is absent. The parent is a Claude transcript, not a Codex parent
rollout, and the child rollout contains no `sub_agent_activity` payload naming
this child as an `agent_thread_id`.

The child UUID first reaches the parent when a later Bash tool use reads the
Codex stdout log. It is present only in two string values: the correlated
`root.message.content[0].content` and `root.toolUseResult.stdout`. The latter has
this shape:

```json
{
  "type": "user", "timestamp": "2026-08-25T21:52:20.713Z",
  "sessionId": "27376f99-8093-4a65-a87f-4e4158f4db69", "agentId": "afbb3cc03cf3fd4f8",
  "message": {"content": [{"type": "tool_result",
    "tool_use_id": "toolu_01SPweiMwmERwKKqdfMfTne3",
    "content": "...\nreasoning effort: xhigh\n...\nsession id: 01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a\n..."}]},
  "toolUseResult": {"stdout": "...\nsession id: 01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a\n...",
    "stderr": "", "interrupted": false}
}
```

There is no named `session_id` or `sessionId` child field on this result. The
text label `session id:` in a captured CLI prelude is a tool-result text blob,
so it is not position 4.

### Third-tier design sketch

The intended hierarchy is:

```text
Claude pane root 27376f99-8093-4a65-a87f-4e4158f4db69
└── Claude wrapper sub-agent afbb3cc03cf3fd4f8
    └── Codex rollout 01a03ae8-a3f8-78e0-9d21-6bfc8f5a335a
```

The first edge is already structural: the wrapper's sub-agent metadata ties it
to the pane root. The second edge needs a typed fact whose parent scope is that
Claude sub-agent and whose child identity is the Codex rollout UUID. Once such
a fact exists, the current admission/reducer shape can exact-match the UUID to
the discovered rollout, admit its path, and synthesize a dispatch edge from the
wrapper scope to the Codex scope.

No G2 position observed in this pair can supply that fact. Position 2 proves
that a Codex process was spawned but has no child UUID; position 3 is absent;
and the only returned UUID is free text. A revised design therefore needs
either a new bounded typed carrier produced by the wrapper, or a newly specified
bounded grammar for a machine-readable Codex result. Timing, working-directory,
prompt, and output-file correlations are useful for this investigation but
must not become production lineage inference. The child `session_meta` also
lacks a typed reference back to the Claude wrapper.

## Pair 2: headless Claude JSON-output invocation and child transcript

### Artifacts examined and pairing

The parent is the top-level Claude transcript
`76748191-dc03-486e-a73e-28ec478c0518.jsonl` in the user's Claude
configuration projects directory. It contains a one-command headless-Claude
probe rather than a named `claude-git-operator` sub-agent. This pair is within
the headless-parent scope and was selected because it exercises the exact
`claude -p --output-format json` contract on which G2a position 4 relies.

The child is `4f270961-f1d0-4ef7-a6af-9520bb42a78a.jsonl` in the same Claude
configuration's projects tree, under the encoded scratch working-directory
project. This is also a positive pairing:

- The parent invoked `claude -p` at `2026-08-23T13:31:25.850Z`; the child's
  first queued record is timestamped `2026-08-23T13:31:29.595Z`.
- The exact prompt, `Reply with exactly: ok`, appears in the command and the
  child's queue and user records.
- The command selected `--model haiku`; the child assistant record reports
  `message.model` `claude-haiku-4-5-20251001` and replies `ok`.
- The child records report `entrypoint` `sdk-cli` and the same scratch working
  directory into which the parent changed before launch.
- The parent result at `2026-08-23T13:31:35.327Z` contains the same child UUID
  in the headless CLI's JSON-output text.

This invocation did not use a secondary `CLAUDE_CONFIG_DIR`, so its child is in
the ordinary Claude projects tree rather than the secondary configuration
directory's projects tree.

### Identity carried by the child

The filename UUID is `4f270961-f1d0-4ef7-a6af-9520bb42a78a`. Each child JSONL
record uses the camel-case root field `sessionId` for that transcript identity.
For example:

```json
{
  "type": "user", "timestamp": "2026-08-23T13:31:31.644Z",
  "message": {"role": "user", "content": "Reply with exactly: ok"},
  "uuid": "d958d6d3-de0c-4a87-8067-c31cd796efcd",
  "entrypoint": "sdk-cli", "promptSource": "sdk",
  "sessionId": "4f270961-f1d0-4ef7-a6af-9520bb42a78a"
}
```

Here `uuid` identifies an individual transcript record; `sessionId` identifies
the child conversation and matches the filename.

### Lineage position and position-4 wire spelling

The position-2 command has the required tool-use nesting but, as expected for a
bare spawn, does not know the child identity:

```json
{
  "type": "assistant", "timestamp": "2026-08-23T13:31:25.850Z",
  "sessionId": "76748191-dc03-486e-a73e-28ec478c0518", "message": {"content": [{
    "type": "tool_use", "id": "toolu_0154bip3G7RQdse2dqEXqJTb", "name": "Bash",
    "input": {"command": "cd [personal scratch path redacted] && timeout 60 claude -p \"Reply with exactly: ok\" --model haiku --output-format json 2>&1 | head -c 900",
      "description": "Verify claude -p JSON output includes session_id (minimal haiku call)"}
  }]}
}
```

This is not a resume command and has no `CLAUDE_CONFIG_DIR` marker. G2a itself
excludes a bare `claude -p` spawn from position 2 because it cannot carry the
assigned child ID. Position 3 is inapplicable and absent because the parent is
not a Codex rollout.

The returned headless-CLI spelling is conclusively snake-case `session_id`.
However, it does **not** sit at `root.toolUseResult.session_id` or
`root.toolUseResult.sessionId`. It is escaped text inside the string at
`root.toolUseResult.stdout`, duplicated in the correlated
`root.message.content[0].content` string:

```json
{
  "type": "user", "timestamp": "2026-08-23T13:31:35.327Z",
  "sessionId": "76748191-dc03-486e-a73e-28ec478c0518",
  "message": {"content": [{"type": "tool_result",
    "tool_use_id": "toolu_0154bip3G7RQdse2dqEXqJTb",
    "content": "{\"is_error\":false,...,\"session_id\":\"4f270961-f1d0-4ef7-a6af-9520bb42a78a\",..."}]},
  "toolUseResult": {"stdout": "{\"is_error\":false,...,\"session_id\":\"4f270961-f1d0-4ef7-a6af-9520bb42a78a\",...",
    "stderr": "[working-directory reset notice]", "interrupted": false}
}
```

The command's `head -c 900` also truncates the stdout string before the JSON
object closes. For this concrete record, a second JSON decode of
`root.toolUseResult.stdout` fails, although the `session_id` member occurs
before the truncation. The root `sessionId` shown above is the **parent**
session, not the child. Thus the only full path to the child bytes in the JSONL
record is `root.toolUseResult.stdout` (or its duplicated tool-result content),
not a typed child-ID field.

This differs from the repository's current deserializer in
`src/provider/claude_facts.rs`. `UserRecord` uses
`#[serde(rename_all = "camelCase")]`, so Rust `tool_use_result` correctly maps
to wire `toolUseResult`. `ToolUseResult` uses the same convention, and its Rust
`agent_id` maps to the real wire field `root.toolUseResult.agentId`; real Agent
tool results on this machine confirm that spelling. Adding Rust `session_id`
beside it, as currently planned, would look for
`root.toolUseResult.sessionId`. That field is absent, and the observed inner
CLI response instead spells `session_id` inside `stdout`. The current plan is
therefore wrong in both location and case for this pair.

`docs/guides/controller-emit-setup.md` correctly states that
`claude -p --output-format json` returns `session_id`, but “retain the returned
`session_id` in the parent transcript” does not make it a typed outer
tool-result field. In the real parent bytes it remains a tool-result text blob,
which G2b says to retire.

## Gate verdict

- Inner Codex family: **FAIL - the child identity appears only in prose.** The
  child UUID occurs in the parent only as `session id: ...` inside strings at
  `root.toolUseResult.stdout` and the correlated tool-result `content`.
  Position 2 has the `codex exec` launch but no child UUID; positions 3 and 4
  do not carry it.
- Headless Claude family: **FAIL - the child identity appears only in prose.**
  The child UUID is labeled by snake-case `session_id`, but only inside the
  Bash stdout/tool-result text string. There is no typed
  `root.toolUseResult.session_id` or `root.toolUseResult.sessionId` field;
  positions 2 and 3 do not carry it.

Per the implementation plan, a FAIL for either family means Task 1 (G2) must
not proceed as specced and the Controller must re-plan. Both examined families
fail that gate.
