# Increment 9: zero-config orchestration visibility (log lane) — design (v2)

Status: v2 after the pre-implementation plan review (REVISE, 8 blockers,
9 should-fix, 3 consider — all confirmed against the shipped source at
e3ce835 and real log artifacts, and all incorporated here). v1 decisions
from the 2026-08-23 brainstorming session are unchanged unless a v2 note
says otherwise. Research record: `~/.research/mageyuki--herdr-top/`.

## 1. Product definition and goals

herdr-top exists to visualize agent orchestration (UC-C): once a herdr
session fans out into sub-agents and dispatched background tasks, "what is
running and in what state" becomes invisible, and herdr-top makes it
visible. Pane-level monitoring alone does not justify the product; it
remains as supporting value.

Increment 9 delivers the UC-C core with ZERO configuration: install the
plugin, open the pane, and the session's agent tree — including headless
workers — appears. The observation channel is the product core; the hook
integration demotes to an optional precision upgrade.

Goals:

1. A live tree of the session's agents: main sessions, dispatched
   sub-agents (with role and human-readable subject), and inner engines
   where lineage evidence exists, from provider session logs alone.
2. Honest lifecycle for headless work: explicit completion where the logs
   record it, `ended_unknown` by inactivity where they do not, and
   liveness derived from transcript appends (roots included).
3. Live one-line activity per running row, output-token totals and mean
   tok/s per row, and per-worker-kind / per-model summary tables.
4. No new configuration, no hook registration, no DB schema change.

Coverage honesty (v2, from measured reality): Claude sub-agents dispatched
through the Agent tool always carry full lineage (`.meta.json`). Inner
codex lineage depends on the child's id appearing SOMEWHERE in the
parent's artifacts (spawn command line, resume invocation, or quoted in a
report); measured on this machine, bare spawn command lines carry the id
in only a small minority of dispatches, while report-quoting workflows and
resumes carry it reliably. Where no id evidence exists, the codex child
rollout is not admitted, so it is never discovered or tailed, never becomes
a run, and is not displayed anywhere, including under Unattached — honest
omission, never inference. A one-line operational convention closes this gap:
have the dispatching agent echo the child's session id into its transcript.

Non-goals: token persistence; hook-lane removal; upstream herdr changes;
the installer/release-blocker work (reserved separately before the first
tag).

## 2. Architecture

The log lane EXTENDS the existing provider subsystem — it does not build a
second one. The shipped `AdapterProviderWorker` (spawned in
`src/herdr/collector.rs` around `:1483` with
`standard_discovery_roots_from_env()` and `RecommendedNotifyFactory`)
already discovers and tails these trees, including the
`<session>/subagents/agent-*.jsonl` topology (`src/provider/claude.rs`
`:249-268`). Increment 9 adds fact extraction and event synthesis INSIDE
that worker's pipeline; there is exactly one watcher, one offset store
(`SourcePosition`), and one parse per byte.

Synthesized facts flow to the collector as new `ProviderEvent` variants
and are applied through the same reducer entry as socket Controller
events:

```
herdr socket ─────────────────────────┐
hook emit (if present) ───────────────┼──▶ reducer ──▶ model / store ──▶ TUI
provider worker (extended: discovery ─┘
  + tailing + facts + synthesis)
```

Dual-lane identity (v2, replaces "natural keys converge"): the shipped
identity code REFUSES to merge two distinct Controller-keyed runs and
refuses a second Controller claim on a bound native session
(`src/identity.rs:404-418`, `:594-600`). Convergence therefore requires
KEY EQUALITY, not merging: the lane synthesizes the hook adapter's raw
Controller keys byte-exactly — `hook:claude-code:<session-uuid>` for a
Claude root, `hook:claude-code:<session-uuid>:agent:<agentId>` for a
Claude sub-agent (the exact recipes the hook adapter emits; pinned by
test against `src/hook_adapter.rs`). Codex sessions synthesize
`RunKey::Native { provider: Codex, sid: <rollout-id> }` claims through the
existing native-binding path (herdr pane detection already produces these
sids for pane roots — verified identical format). Synthesized event ids
are DETERMINISTIC —
`log:<artifact-basename>:<record-ordinal>:<kind-slug>:<target-id>[:<per-record-sequence>]`
— so the existing durable event ledger deduplicates re-reads across restarts.
The optional sixth field is present on `activity` events and absent from
subject and lifecycle events. The five-field identity for kinds without a
sequence remains stable if fact types are added or reordered; activity's
per-record sequence makes repeated instances at the same ordinal distinct.
Ids never use the reserved `prov:` prefix (`src/reducer.rs:496` rejects it).

Persistence boundary: no schema change. Token/effort aggregates live in a
transient telemetry map inside the model (`src/model/entities.rs`,
serde-skipped, never persisted), published by the reducer with the same
coherence as other model state so the projection reads it without new
channels and without violating the paint/rebuild separation. Reopen
provenance (see §4) is reconstructed at startup from the already-persisted
`events.source` column (`src/store/schema.rs:579-590`) — a read-model
query, not a migration.

## 3. Discovery and watching

Roots and membership as v1: pane `agent_session.value` admits roots
(Claude uuid = transcript filename; codex value = rollout id — both
verified); descendants by explicit evidence only; `CLAUDE_CONFIG_DIR`
values captured from parent records add lineage-scoped derived roots;
evidence-free artifacts stay out of the tree.

Explicitly EXCLUDED from discovery and tailing: `tool-results/`
directories (raw tool output), and any file class not named in §5.

Backfill anchor (v2, resolves a plan/spec conflict): the anchor is
`max(earliest own-DB event for this session, now − HERDR_TOP_BACKFILL_WINDOW_MS)`
— the window is a hard scan bound; the DB can only narrow it, never widen
it. Lineage evidence admits an artifact's identity for descendant
discovery and attachment, but reading that artifact still honors the
anchor. An evidence-admitted artifact whose mtime is strictly older than
the anchor is not read. Pane-root artifacts (the transcript or rollout of
an admitted pane session) are likewise exempt from the anchor — a live
but idle pane's own artifact must not become invisible when its mtime
falls behind the window.

Known accepted residual: within-window UUID echoes can still cause a
false attachment. A fresh tool output that merely names a concurrently
active unrelated session can admit that session's artifact while its
mtime remains inside the window. Window-bounding removes the multi-day
replay and limits the blast radius, but it does not eliminate the class.
A full fix requires dispatch-shaped context parsing that distinguishes a
UUID in an actual dispatch or spawn record from one merely embedded in
tool output; that parsing is deliberately out of scope here.

Watching: the existing worker's notify + rescan machinery; per-file
`SourcePosition` offsets; polling degradation. Idle cost stays
event-driven.

Outside-pane execution and session isolation: unchanged from v1.

## 4. Derivation rules (facts → synthesized events)

| # | Fact (source) | Synthesis |
|---|---|---|
| 1 | Claude child appears: `subagents/agent-<id>.jsonl` + `.meta.json` | `dispatch` (parent = containing session's run) + `task_started`; kind = `agentType`; subject = `description`; Controller key per §2 recipe |
| 2 | Claude root session | connect + enrich: subject = latest `ai-title` in FILE ORDER (the record has no timestamp — verified keys `aiTitle`/`sessionId`/`type` only), else cwd basename; model and effort from assistant records (`message.model`; top-level `effort` — verified present on 1579/1579 assistant records) |
| 3 | Codex session appears: rollout `session_meta` | `task_started` via native-key claim; kind from `originator`; model/effort/sandbox from `turn_context` — PER TURN (a resumed rollout changes model and effort mid-file — verified; display the latest turn's values; Detail may list per-turn history) |
| 4 | Codex child lineage | id-pattern extraction (§5 carve-in) over parent artifacts matched against discovered rollout ids → `dispatch`; no match → Unattached |
| 5 | Headless Claude child lineage | same mechanism; JSON-output invocations verifiably print `session_id` |
| 6 | Codex internal thread | `source.subagent` has TWO shapes (verified): `{"other": <name>}` (no parent id) → agent node named `<name>` under the rollout's run; `{"thread_spawn": {parent_thread_id, depth, agent_path, agent_nickname, agent_role}}` → agent node under the parent thread's run with the nickname/role as its label. A rollout may contain a second `session_meta` (copied parent metadata — verified); only the first is identity |
| 7 | Current activity | Claude: newest `tool_use` name + summary param (repo-relative paths; codex `CommandExecution.cwd` is a `file://` URI — strip the scheme before relativizing). Codex: AgentMessage `phase=="commentary"` text at `item.content[0].text`, else CommandExecution sanitized head — `command` is an ARGV ARRAY (verified); sanitize the script element, not the array head |
| 8 | Tokens (v2, exact definitions) | TOK = OUTPUT tokens; tok/s = output tokens / wall-clock (cumulative mean). Claude: deduplicate usage samples by `message.id` (verified: identical records recur up to 7×), then sum `output_tokens`; the full breakdown (input, cache read/creation — which dwarf output ~350×) goes to the Detail overlay only. Codex: `token_count.total_token_usage` is cumulative WITHIN a turn and RESETS across turns (verified) — accumulate per-turn deltas using `last_token_usage`, closing each turn at `task_complete` |
| 9 | Completion: codex tail `task_complete`; Claude child = parent notification with `status=completed`; `status=failed` (verified 7 occurrences) maps to `Failed`; codex `turn_aborted` maps to `Cancelled` | `complete`/`failed`/`cancelled` via the grace rule |
| 10 | Death backstop: append silence past `HERDR_TOP_HEADLESS_INACTIVITY_MS` | the lane synthesizes its OWN `ended_unknown` transition (v2: the existing observation close path is unreachable for lane-created runs because synthesized controller events set `has_controller_task_state_event`, and `src/reducer.rs:1626-1638` early-returns on it). The lane's close honors the same guards: never on terminal, never on dismissed, never when pane-occupied (has an execution), and never with non-lane-sourced task-state evidence |
| 11 | Liveness: any append | refresh the run's `updated_at` — ROOTS INCLUDED; dismissal is checked BEFORE touching (touch clears `dismissed_at_ms` on the non-terminal branch) |

Completion grace and reopen (unchanged mechanism, v2 provenance):
`task_complete` → grace (`HERDR_TOP_COMPLETE_GRACE_MS`) → `complete`.
Same-file append after completion may REOPEN (complete → running,
one-way) IFF the terminal state was itself log-synthesized. Provenance is
determined from the persisted `events.source` of the terminal event
(restored via a read-model query at startup) — surviving restarts without
schema change. Hook-sourced terminals are never reopenable; the shipped
stale-event guard (`src/reducer.rs:2054-2061`) stays intact for every
non-lane source.
The pre-existing `ended_unknown -> running` resume transition is distinct from this provenance-gated `complete -> running` reopen.

Stall (⚠) and ghost-run window: unchanged from v1.

## 5. Privacy allowlist

Principles (ADR ships with implementation):

1. Only enumerated fields are read; unknown records are skipped.
2. Parsing uses TYPED allowlist envelopes (serde structs with only the
   allowlisted fields, matching the shipped adapter pattern at
   `src/provider/claude.rs:12-30`) — NOT untyped `serde_json::Value`,
   which would materialize entire records including bodies (codex
   `CommandExecution` carries full `stdout`/`stderr`/`aggregated_output` and is
   the largest `item_completed` class by byte volume; `Reasoning` is the most
   numerous, while multi-kilobyte reasoning bodies live in `response_item`
   reasoning `encrypted_content` — all verified).
3. Three narrow carve-ins, each pattern-extraction-only (input text is
   never retained, displayed, or logged):
   - ID EXTRACTION: raw transcript lines are regex-scanned for
     uuid-shaped tokens; a token is used ONLY if it matches a discovered
     rollout/transcript id (making it explicit evidence, not inference)
     or a `CLAUDE_CONFIG_DIR=` assignment. Nothing else leaves the scan.
   - COMPLETION STATUS: `queue-operation.content` (a free-text field that
     also carries user prompts — verified up to 34 KB) is scanned only
     for `<task-notification>` blocks, extracting exactly the `task-id`
     and `status` tags.
   - COMMENTARY: codex AgentMessage `phase=="commentary"` text at
     `item.content[0].text`, one line, ≤60 chars.
4. Changes are ADR revisions.

Read lists otherwise as v1 (Claude: record type/timestamp/sessionId/cwd/
version, aiTitle, assistant model + effort + usage numerics with
`message.id` for dedup, tool_use name + Bash/Agent description, Agent
tool-result agentId; meta.json all four fields; codex: session_meta
identity set, turn_context model/effort/sandbox per turn, token_count
numerics, lifecycle events, item_completed types/timestamps/process_id —
`process_id` is a JSON STRING, verified). Displayed and never-read lists
as v1. `tool-results/` directories are never opened. Fixtures MUST
include realistic message bodies so never-materialized tests are
non-vacuous, and the tests instrument the read boundary (assert zero
opens of excluded files; assert extractor types cannot hold body text).

## 6. Placement and tree

Order (v2, preserves a pinned shipped behavior): live-execution pane →
latest ENDED execution's pane (existing fallback, pinned by
`src/tui/view.rs:3019`) → dispatch parent (new, for runs with no
execution history) → Unattached. `[dispatched by:]` annotation stays on
pane-placed runs only. If a dispatch parent is not default-visible
(dismissed/expired), its headless children fall back to Unattached for
that frame — a hidden parent never hides its children. Cycle safety:
malformed edges bail to Unattached. Three explicit levels and deeper;
same-agent resume continues the same branch.

## 7. Row format and responsiveness

As v1 (glyphs, subject chain, live line, columns, shedding, indent
compression) with v2 corrections:

The dependency DAG renders the status glyph plus label without the live line.

The fixed documentation columns have widths MODEL 11, EFF 5, TOK 5, TOK-S 5, and TIME 6; bands are selected from the Execution tree inner width as 120 or wider for all five, 104–119 for EFF/TOK/TOK-S/TIME, 90–103 for TOK/TOK-S/TIME, 76–89 for TOK/TIME, 62–75 for TIME, and below 62 for none, yielding the ratified narrow-screen drop order MODEL, EFF, TOK-S, TOK, TIME.

- EFF is real on BOTH providers (Claude assistant records carry top-level
  `effort` — verified), so the column shows actual values, `—` only when
  genuinely absent.
- TOK/TOK-S render output tokens per §4-8; Detail shows the full
  breakdown including cache traffic.
- Committed filter indicator: `footer_line` currently receives only a
  width (`src/tui/view.rs:443`, called at `:144`) — plumb state through;
  the draft editor paints the same row (`:479`), so the draft view wins
  while active and the committed indicator renders otherwise.
- The `up:` header field becomes shrink-and-drop-last.

## 8. Summary and Detail

As v1. Detail additions now include the per-turn model/effort history for
resumed codex rollouts and the token breakdown.

## 9. doctor and configuration

Checks as v1 (`log_lane.readable`, `log_lane.coverage`,
`log_lane.freshness`), with the read-model plumbing named explicitly: the
lane reports its state into `RuntimeDiagnosticsSnapshot`
(`src/diagnostics/mod.rs`) exactly as `enrichment_counters` does, and
doctor consumes it through the existing Controller status response —
`src/doctor.rs` and `src/diagnostics/mod.rs` are in scope. Freshness is
measured from the watcher's own observation timestamps, not file mtimes,
so a dead watcher cannot report fresh.

Environment variables: as v1's table, with explicit parsing semantics:
UTF-8 decimal i64; absent, malformed, non-positive, or overflowing values
fall back to the default silently (the resolved values are attached to an
INFO-level startup trace event, which the shipped WARN-level subscriber filters
out of the default log).

## 10. Integrated defect fixes

1. Gap-execution growth: reuse keyed on (pane, terminal, resolved
   occupant — run or native session); a changed occupant mints a new
   execution (the upsert rewrites ownership — `src/store/mod.rs:1778-1782`
   — so occupant identity must be part of the key); stop re-persisting
   already-terminal executions in `reconcile_gap_inner`
   (`src/reducer.rs:773+`, mint site `:829-830`).
2. Watchdog probe volatility: exclude volatile agent/execution state from
   the probe equality at `src/herdr/collector.rs:2203`.
3. Hook-only expiry: protection via restored executions + liveness touch;
   incident-shaped regression test. A structural test pins the
   six-reducer-owning-selects invariant (`receive_operator_command` call
   sites) so lane wiring cannot silently add or drop one.

## 11. Documentation repositioning

As v1, plus: document the lineage coverage boundary and the one-line
convention that closes it (§1); document TOK = output tokens.

## 12. Testing strategy

As v1, strengthened per review: fixtures carry realistic bodies;
boundary tests instrument file opens (zero opens of excluded/unadmitted
files — not merely zero facts); summary/doctor tests drive real
extraction paths rather than hand-built totals; freshness tests kill the
watcher and assert staleness; token tests pin the dedup (Claude) and
turn-delta (codex) arithmetic against the fixture numbers; the live smoke
(six-point demo) is an owned plan task executed and recorded before
publication.

## 13. Sequencing

(1) fixtures + ADR + baselines; (2) typed extraction (Claude, codex);
(3) worker extension: admission, tailing, id-extraction; (4) synthesis:
keys/ids + lifecycle events; (5) lifecycle state: grace, reopen
(events.source read-model), inactivity close, liveness; (6) telemetry map
+ tokens; (7) placement; (8) rows + stall + ghost; (9) columns +
shedding + filter indicator + header; (10) Summary/Detail + doctor +
diagnostics plumbing; (11) defect fixes; (12) docs; (13) live smoke.
