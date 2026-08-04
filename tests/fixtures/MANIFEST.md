# Fixture provenance and sanitization

Raw probe transcripts are never committed. The fixtures in this directory retain only the structural evidence needed by later vertical-slice tests.

## Wire family

The five files in `tests/fixtures/wire/` are sanitized derivations of local Herdr protocol probe transcripts captured in 2026-08 against Herdr 0.8.0 and protocol 19.

| File | Evidence retained |
| --- | --- |
| `p0-conn-semantics.jsonl` | One request is accepted per connection. The server closes the connection after each response and sends no greeting banner. |
| `p1-snapshot.jsonl` | `session.snapshot` returns `{type:"session_snapshot", snapshot:{version:"0.8.0", protocol:19, workspaces, tabs, panes, layouts, agents, focused_*}}`. `PaneInfo` carries `terminal_id`, `revision`, and optional `agent_session{source,agent,kind,value}`. |
| `p2-subscribe-push.jsonl` | The subscription acknowledgement is `{"type":"subscription_started"}`. Subscription type names use dot form; pushes are exactly `{event,data}` with underscore event names. |
| `p4-terminal-id-move.jsonl` | Moving a pane across workspaces preserves `terminal_id`, changes the public pane ID, and emits a `pane_moved` push. |
| `p6-cold-restart.jsonl` | A cold restart regenerates every `terminal_id`. Restored panes with a latched `agent_session` and no live process receive synthesized `agent=...` and `agent_status=idle` values. |

The wire sanitization pass applied these replacements:

- Capture user's OS username → `user`
- Capture user's home directory → `/home/user`
- Repository path segments → `proj`

`CAPTURE_USERNAME` denotes the OS username on the machine where the probes ran. `CAPTURE_HOME` denotes that user's expanded home directory. The fixture-wide forbidden-literal checks are:

```sh
! grep -RF "$CAPTURE_USERNAME" tests/fixtures/
! grep -RF "$CAPTURE_HOME" tests/fixtures/
```

## Provider family

The five files in `tests/fixtures/provider/` were adapted from the research staging manifest. The same replacement rules were re-applied during import, and one residual repository-path segment family was additionally replaced with `proj`.

| File | Evidence retained |
| --- | --- |
| `codex-depth2-root.jsonl` | Root `session_meta` records without `agent_path`, `sub_agent_activity` events, and unknown-record `{"type": ..., "redacted": true}` stubs for tolerance tests. |
| `codex-depth2-child.jsonl` | Depth-1 `session_meta`, where `agent_path` has two segments and `parent_thread_id` points to the root. |
| `codex-depth2-grandchild.jsonl` | Depth-2 `session_meta`, where `agent_path` has three segments; this is the only locally evidenced depth-2 chain. |
| `claude-depth1-subagent.jsonl` | The `isSidechain`/`agentId`/parent `sessionId` structure and redacted message stubs. |
| `claude-depth1-parent.jsonl` | Main-chain records, including `toolUseResult` agent-spawn results, limited to the first 50 records. |

The provider staging sanitization retains:

- In Codex files: the structural `session_meta` payload, `sub_agent_activity` events, `inter_agent_communication_metadata.trigger_turn`, and no more than three redacted stubs per unknown record type.
- In Claude files: topology and attribution keys; `message` reduced to `{role, [{"type":"redacted"}]}`; and `toolUseResult` reduced to agent-spawn structural fields.
- In all provider files: home-directory values replaced with `/home/user`.

Fixtures deeper than the locally evidenced topology, such as a Codex depth-3 `agent_path`, must be authored synthetically and marked unevidenced.
