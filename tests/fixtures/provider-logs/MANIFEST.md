# Provider-log fixtures

These fixtures mirror the JSON shapes of Claude Code 2.1.x transcripts and
Codex CLI 0.149.x rollout files. Every conversation, prompt, assistant,
reasoning, command, output, summary, title, repository, host, path, identifier,
and timestamp value is a synthetic placeholder created for this fixture set.
No real content was copied.

All identities are fictional. Home paths use `/home/user`, repository paths
use `/home/user/git/example/herdr-top`, and the only remote URL uses the
reserved `.invalid` domain. The large message, reasoning, prompt, and command
output bodies are intentional: they make privacy-boundary tests non-vacuous by
ensuring excluded fields would be material if accidentally deserialized.

## Claude fixtures

### `claude-session.jsonl`

Mirrors a main-session transcript. It contains `ai-title`, `user`, and
`assistant` records and exercises the Claude read list:

- record `type`, `timestamp`, `sessionId`, `cwd`, and transcript `version`;
- `aiTitle`;
- assistant `message.id`, `message.model`, top-level `effort`, and numeric
  `message.usage` fields;
- `tool_use` name plus Bash and Agent `description` values; and
- an Agent `tool_result` whose top-level `toolUseResult.agentId` identifies the
  child.

The assistant message id `msg_02SyntheticStreamChunk` occurs twice with the
same usage sample so token deduplication is testable. The Bash spawn command
contains `CLAUDE_CONFIG_DIR=/home/user/.claude-secondary` and quotes Codex
rollout id `6f9bdfa0-1502-4a37-97aa-c45591141130`; that id is the identity in
`codex-exec.jsonl`.

### `claude-subagent-meta.json`

Mirrors `agent-<agentId>.meta.json` with the complete four-field shape:
`agentType`, `description`, `toolUseId`, and `spawnDepth`.

### `claude-subagent.jsonl`

Mirrors the real-world file
`agent-0392548834e4a5dc.jsonl`. It contains sidechain `user` and `assistant`
records, an `agentId`, the parent session id, model, effort, usage, and a large
synthetic assistant report.

The child id `0392548834e4a5dc` is identical in this conceptual filename, the
parent Agent `tool_result` in `claude-session.jsonl`, and the task metadata
represented by `claude-subagent-meta.json`. Its spawning tool-use id is
`toolu_02SyntheticAgentSpawn` in both the parent transcript and metadata.

### `claude-queue-notifications.jsonl`

Mirrors main-session `queue-operation` records. One `enqueue` contains a
multi-kilobyte ordinary user prompt. Two more contain the real
`<task-notification>` tag layout: `task-id`, `tool-use-id`, `output-file`,
`status`, and `summary`. The statuses include both `completed` and `failed`.
Only `task-id` and `status` are privacy-carve-in outputs.

Every record in each Claude JSONL fixture uses session id
`13f03635-c1f6-46e2-8e52-83d217b6f01c`.

## Codex fixtures

### `codex-exec.jsonl`

Mirrors one Codex exec rollout. It exercises the Codex read list:

- the `session_meta` identity set, including `id`, `session_id`, timestamp,
  cwd, originator, `cli_version`, source, thread source, model provider, and
  git metadata;
- per-turn `turn_context` model, effort, and sandbox policy;
- `task_started`, `token_count`, and `task_complete` lifecycle and token
  records, including `last_token_usage` numerics; and
- `item_completed` timing plus `UserMessage`, `Reasoning`, `AgentMessage`, and
  `CommandExecution` item types.

The commentary AgentMessage is a single 41-character line. The Reasoning item
has a large synthetic `raw_content` body. The CommandExecution item carries
synthetic `stdout`, `stderr`, and `aggregated_output`; its `process_id` is the
JSON string `"42420"`. This file also carries the internal-subagent source
shape whose `source.subagent` is exactly `{"other":"guardian"}`.

Its rollout id is `6f9bdfa0-1502-4a37-97aa-c45591141130`, matching the quoted
id in the Claude parent spawn command.

### `codex-exec-resume-appended.jsonl`

Mirrors one append-resumed rollout. Rollout id
`745480a2-5bdc-483f-ab53-0b4fabc01781` has two complete
`task_started`-through-`task_complete` turn pairs. The first turn context uses
model `gpt-5.6-terra` at effort `low`; the appended turn context uses model
`gpt-5.6-sol` at effort `xhigh`. Each turn has its own `last_token_usage`, so a
consumer can test per-turn token deltas and closure at `task_complete`.

### `codex-internal-subagents.jsonl`

Mirrors an internal thread rollout whose first `session_meta` uses the
`source.subagent.thread_spawn` object with `parent_thread_id`, `depth`,
`agent_path`, `agent_nickname`, and `agent_role`. Child thread id
`273e0c2b-4af4-4014-b24c-8b0d03ba8905` points to parent thread id
`69c67f5c-9d6d-4976-8465-5e6a31df2c0b`. The file also includes
`SubAgentActivity` and `CollabAgentToolCall` item types using those identities.

The second line is the observed copied-parent `session_meta` shape, with the
parent id and CLI source. Thus this file carries both the dual-`session_meta`
case and the `thread_spawn` case. Only the first metadata record is the
rollout identity.

## Privacy coverage

The typed read fields above are the only fields intended for ordinary parsing.
The fixtures also contain fields that must never be materialized: Claude user,
assistant, thinking, prompt, command, and tool-result bodies; Codex user and
reasoning bodies; Codex command scripts; and CommandExecution `stdout`,
`stderr`, `aggregated_output`, and formatted output. Files under
`tool-results/` are outside this fixture set because those directories must
never be opened.

Three narrowly bounded pattern-only cases are present: discovered-id and
`CLAUDE_CONFIG_DIR=` extraction from raw Claude lines, `task-id` and `status`
extraction from task-notification blocks, and one-line Codex commentary text of
at most 60 characters. Input text for those scans is never retained,
displayed, or logged.
