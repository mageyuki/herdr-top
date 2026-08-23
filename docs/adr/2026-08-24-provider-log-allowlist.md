# Provider log privacy allowlist

## Status

Accepted

## Date

2026-08-24

## Context

The provider log lane derives run identity, lineage, lifecycle, current
activity, and token telemetry from Claude Code transcript files and Codex
rollout files. Those files also contain full user prompts, assistant responses,
reasoning, commands, tool output, and other text that herdr-top neither needs
nor should materialize.

The risk is concrete. A Codex `CommandExecution` item carries complete
`stdout`, `stderr`, and `aggregated_output`. `CommandExecution` is the largest
`item_completed` class by byte volume, while `Reasoning` is the most numerous;
multi-kilobyte reasoning bodies live in `response_item` reasoning records as
`encrypted_content`. Deserializing a whole record into an untyped
`serde_json::Value` would materialize those bodies even if later code discarded
them. Claude queue-operation content is also free text: the same field carries
ordinary user prompts and machine-readable task-notification blocks.

The shipped Claude adapter establishes the safe parsing pattern in
`src/provider/claude.rs`: small serde structs hold only enumerated fields. The
log lane needs a precise extension of that pattern and an explicit boundary for
the few facts that can only be recognized inside text.

## Decision

Provider logs are parsed only through typed allowlist envelopes: serde structs
contain only the fields named below. Unknown record types and unknown fields
are skipped. Full records are never deserialized into untyped
`serde_json::Value` values.

### Claude read list

The ordinary typed reader may read:

- record `type`, `timestamp`, `sessionId`, `cwd`, and transcript `version`;
- `aiTitle` from `ai-title` records;
- assistant `message.id`, `message.model`, top-level `effort`, and numeric
  `message.usage` fields;
- `tool_use.name`, plus `input.description` for Bash and Agent tool uses;
- `toolUseResult.agentId` for an Agent tool result; and
- all four subagent metadata fields: `agentType`, `description`, `toolUseId`,
  and `spawnDepth`.

### Codex read list

The ordinary typed reader may read:

- the `session_meta` identity set: rollout `id`, `session_id`, timestamp, cwd,
  originator, `cli_version`, source and thread source, model provider, git
  identity fields, and the two typed `source.subagent` forms;
- per-turn `turn_context` model, effort, and sandbox policy;
- numeric `token_count` fields, including `last_token_usage` and the full input,
  cached-input, cache-write-input, output, reasoning-output, total-token, and
  context-window breakdown;
- lifecycle event type, turn identity, and timestamps for `task_started`,
  `task_complete`, and `turn_aborted`;
- `item_completed` item type and event timestamps, plus string `process_id`;
  and
- the typed CommandExecution command argv and cwd needed to produce a
  sanitized current-activity head.

`process_id` is a JSON string, not a number.

### Pattern-extraction-only carve-ins

Exactly three text scans are permitted. They do not create a general license to
retain or parse message bodies.

1. **ID EXTRACTION:** Raw transcript lines may be pattern-scanned for
   UUID-shaped tokens and `CLAUDE_CONFIG_DIR=` assignments. A UUID-shaped token
   is emitted only when it exactly matches an already discovered rollout or
   transcript id; unmatched tokens are discarded. For a configuration
   assignment, only the assignment value is emitted. Nothing else leaves the
   scan.
2. **COMPLETION STATUS:** `queue-operation.content` may be scanned only for
   `<task-notification>` blocks. Within a matching block, the scanner extracts
   exactly the `task-id` and `status` tags. Ordinary prompt text and every other
   tag or byte are discarded.
3. **COMMENTARY:** For a Codex AgentMessage whose `phase` is exactly
   `commentary`, the scanner may extract text at `item.content[0].text` only
   when it is one line and at most 60 characters. Other phases, additional
   content entries, multiline text, and longer text are discarded.

The full input text presented to any carve-in is never retained, displayed, or
logged. Only the narrowly extracted output described above may leave the scan;
of those outputs, only accepted commentary is display text.

### Displayed fields

The UI may display only facts derived from the typed read lists and accepted
carve-in outputs:

- Claude's latest file-order `aiTitle`, otherwise the cwd basename; Claude
  subagent `agentType` and Controller-authored `description`;
- Codex originator and typed internal-thread nickname or role;
- the latest turn's model, effort, and sandbox summary, with per-turn history
  confined to Detail;
- lifecycle state and evidence-backed lineage;
- current activity from a Claude tool name and Bash or Agent description, an
  accepted Codex commentary line, or a sanitized CommandExecution head with
  any path rendered repository-relative; and
- output-token count and output-token rate in the main view, with the full
  token breakdown in Detail only.

Identifiers and configuration-directory values extracted for correlation are
used as evidence for discovery and lineage, not as additional free-text display
fields.

### Never-read fields and files

The reader never materializes Claude user prompts, assistant text or thinking,
Agent prompts, Bash command bodies, ordinary queue-operation text, tool-result
bodies, or any other conversation body. It never materializes Codex user
messages, reasoning summaries or raw reasoning, base instructions,
non-qualifying AgentMessage text, CommandExecution `stdout`, `stderr`,
`aggregated_output`, or formatted output, MCP arguments or results, file-change
output, extension results, or unknown item bodies.

The categorical exclusion also names `response_item` bodies (reasoning
`encrypted_content`, `custom_tool_call_output` `output`, and message content),
`world_state` records, and `token_count` `rate_limits` as never-read data.

`tool-results/` directories are never opened, enumerated, tailed, or used for
fallback extraction.

### Token definition

`TOK` and `TOK-S` are output-token measures. `TOK` is cumulative output tokens;
`TOK-S` is cumulative output tokens divided by wall-clock time.

Claude usage samples are deduplicated by `message.id` before summing
`output_tokens`, because identical assistant records can recur. Codex token
counts are accumulated as per-turn deltas from `last_token_usage`, and each
turn is closed at `task_complete` because cumulative totals reset across
turns. Input, cached-input, cache-creation or cache-write-input,
reasoning-output, and total-token values are available only in the Detail view;
they do not redefine `TOK` or `TOK-S`.

Any expansion or other change to this allowlist requires a revision to this
ADR.

## Alternatives considered

### Deserialize full records into `serde_json::Value`

This would simplify exploratory field access but would materialize sensitive,
multi-kilobyte bodies before filtering. It was rejected because the privacy
boundary must exist at deserialization, not after it.

### Deserialize full provider structs and discard unused fields

This would give compile-time structure but would still read fields outside the
product requirement and would make future provider additions silently enter
memory. It was rejected in favor of narrow envelopes whose field set is itself
the allowlist.

### Read tool-result files to improve status or activity

Tool-result files contain raw command output and are unnecessary for the
evidence model. This was rejected; the directory exclusion is categorical.

## Consequences

Provider format drift can remove allowlisted facts without exposing new bodies:
unknown fields and records remain skipped, while version-prefix and record-type
baselines signal review. Fixtures retain realistic large synthetic bodies so
tests can prove excluded data is not materialized.

The lane can display useful identity, status, activity, and output-token
telemetry, but it deliberately gives up information available only inside
conversation, reasoning, or tool-output bodies. New display requirements must
first justify and document a narrow allowlist revision.
