# Increment 9: Log-Lane Orchestration Visibility Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

v2 after the pre-implementation plan review (REVISE): all 8 blockers,
9 should-fix, and 3 consider items are incorporated. Task boundaries were
re-cut (the former Task 5 split three ways), anchors corrected, and every
test the review called vacuous was replaced with a boundary-instrumented
version.

**Goal:** Zero-config orchestration visibility: the session's agent tree,
live activity lines, honest lifecycle, and output-token metrics derived
from provider session logs, with no hook registration and no schema
change.

**Architecture:** Extend the existing `AdapterProviderWorker` pipeline
(one watcher, one offset store, one parse per byte) with typed allowlist
extraction and event synthesis feeding the existing reducer; a transient
telemetry map carries tokens/effort to the projection; two shipped defects
(gap-execution growth, probe volatility) are fixed in-increment.

**Tech Stack:** Rust (MSRV 1.97.1), tokio, ratatui, rusqlite, notify;
no new dependencies.

**Spec:** docs/internal/superpowers/specs/2026-08-24-increment-9-log-lane-orchestration-design.md (v2)

## Global Constraints

- No DB schema change. Telemetry (tokens/effort) is a serde-skipped
  transient map in the model; reopen provenance is reconstructed from the
  persisted `events.source` column (`src/store/schema.rs:579-590`) by a
  read-model query at startup.
- Privacy: TYPED allowlist envelopes only (pattern:
  `src/provider/claude.rs:12-30`); never `serde_json::Value` over full
  records. Three pattern-extraction carve-ins (ids; task-notification
  status; commentary ≤60 chars) per spec §5. `tool-results/` is never
  opened. Fixtures carry realistic bodies.
- Identity: synthesized Controller keys reproduce the hook adapter's
  byte-exact recipes (`hook:claude-code:<uuid>`,
  `hook:claude-code:<uuid>:agent:<agentId>` — pin against
  `src/hook_adapter.rs`); codex sessions claim
  `RunKey::Native { Codex, <rollout-id> }`. Synthesized event ids use the
  semantic form
  `log:<artifact-basename>:<record-ordinal>:<kind-slug>:<target-id>`. The id
  keys on (artifact basename, record ordinal, kind, target identity). This
  avoids positional-id drift when fact types are added or reordered; the same
  target and kind repeated within one record is one semantic event. Ids are
  never `prov:`-prefixed (`src/reducer.rs:496` rejects that prefix).
- Evidence only; no inference. Codex lineage uses the id-extraction
  carve-in matched against discovered artifacts; no match → Unattached.
- TOK/TOK-S = OUTPUT tokens (Claude usage deduped by `message.id`; codex
  per-turn deltas via `last_token_usage`, turns closed at
  `task_complete`). Full breakdown in Detail only.
- Env vars (all i64 ms; UTF-8 decimal; absent/malformed/non-positive/
  overflow → default): `HERDR_TOP_STALL_WARN_MS` 300000;
  `HERDR_TOP_HEADLESS_INACTIVITY_MS` 600000;
  `HERDR_TOP_COMPLETE_GRACE_MS` 30000; `HERDR_TOP_GHOST_VISIBILITY_MS`
  300000; `HERDR_TOP_BACKFILL_WINDOW_MS` 86400000. Backfill anchor =
  `max(earliest own-DB event, now − window)`.
- CI-exact gates per task: `cargo fmt --check`; `cargo clippy --locked
  --all-targets --all-features -- -D warnings`; `cargo check --locked
  --all-targets`; `cargo test --locked --all-targets --all-features`;
  `cargo test --locked --doc`.
- Invariants: `i4_idle_deadline_cache_avoids_projection_rebuilds` stays
  green and non-vacuous; the six reducer-owning operator-command selects
  (`receive_operator_command` sites, currently `src/herdr/collector.rs`
  lines 1631/1862/2119/2450/2633/6211) stay exactly six — Task 11 adds a
  structural test pinning this.

## File Structure

- `src/provider/facts.rs` (new): `LogFact`, `SessionScope`, sanitizers.
- `src/provider/claude_facts.rs`, `src/provider/codex_facts.rs` (new):
  typed extraction.
- `src/provider/lane.rs` (new): admission/evidence graph, synthesis
  state (grace, inactivity, liveness, token accumulation), key/id
  recipes.
- `src/provider/mod.rs`, `src/provider/claude.rs`: extend the existing
  worker (`AdapterProviderWorker` path) — no parallel stack.
- `src/herdr/collector.rs`: route new `ProviderEvent` variants; probe
  fix.
- `src/reducer.rs`: liveness touch, lane close, reopen, gap-execution
  fix.
- `src/store/mod.rs`: `events.source` read-model query for reopen
  provenance (no schema change).
- `src/model/entities.rs`: transient telemetry map (serde-skipped).
- `src/tui/{view,projection,app}.rs`, `src/activity.rs`, `src/main.rs`,
  `src/diagnostics/mod.rs`, `src/doctor.rs`: display, config, health.
- `docs/adr/2026-08-24-provider-log-allowlist.md`,
  `tests/fixtures/provider-logs/`, schema-review baselines.

---

### Task 1: Fixtures with realistic bodies, allowlist ADR, format baselines

**Files:**
- Create: `tests/fixtures/provider-logs/{claude-session.jsonl,claude-subagent-meta.json,claude-subagent.jsonl,claude-queue-notifications.jsonl,codex-exec.jsonl,codex-exec-resume-appended.jsonl,codex-internal-subagents.jsonl,MANIFEST.md}`
- Create: `docs/adr/2026-08-24-provider-log-allowlist.md`
- Modify: `scripts/review-herdr-protocol.sh`; add
  `tests/fixtures/herdr-schema/{claude-log-baseline.json,codex-log-baseline.json}`
- Test: extend `tests/schema_review_script.rs`

**Interfaces:** fixture paths consumed by Tasks 2–6; ADR text quoted by
Task 12.

- [ ] **Step 1:** Capture sanitized fixtures INCLUDING realistic bodies
  (multi-KB assistant text, CommandExecution with stdout/stderr,
  Reasoning items, queue-operation records mixing user prompts and
  task-notification blocks with `completed` AND `failed` statuses). The
  codex resume fixture: one rollout, two `task_started`…`task_complete`
  turn pairs, two DIFFERING `turn_context` records (model and effort
  change — mirrors verified reality). Include one rollout with a second
  copied `session_meta`, one with `source.subagent = {"other":"guardian"}`,
  and one with the `thread_spawn` object shape. Sanitize identities
  (user/example-host/`/home/user`) with internal consistency (parent
  spawn line quotes the codex fixture's rollout id).
- [ ] **Step 2:** Write the ADR from spec §5 (typed envelopes, three
  carve-ins, tool-results exclusion, TOK definition).
- [ ] **Step 3:** Extend the review script with the two log baselines
  (Claude `version` 2.1.x, codex `cli_version` 0.149.x) using the
  existing multiset semantics.
- [ ] **Step 4:** Failing-first `log_baselines_cover_fixture_record_types`
  plus `log_baseline_detects_novel_record_type` (tempdir copy + injected
  record → non-zero exit).
- [ ] **Step 5:** Gates; commit `test: provider log fixtures with bodies, allowlist ADR, baselines`.

### Task 2: Typed Claude extraction

**Files:**
- Create: `src/provider/facts.rs`, `src/provider/claude_facts.rs`
- Modify: `src/provider/mod.rs` (module decls)
- Test: unit tests in the new files

**Interfaces (produces):**
```rust
pub enum SessionScope { ClaudeRoot(String), ClaudeSubagent { parent: String, agent_id: String }, Codex { rollout_id: String } }
pub enum LogFact {
    Append { scope: SessionScope, at_ms: i64 },
    AiTitle { session_id: String, title: String },            // latest = file order
    SubagentAppeared { parent: String, agent_id: String, agent_type: String, description: String },
    SubagentEnded { parent: String, agent_id: String, failed: bool },
    Activity { scope: SessionScope, at_ms: i64, line: String }, // sanitized, ≤60
    Usage { scope: SessionScope, at_ms: i64, sample_id: String, output_tokens: u64, model: Option<String>, effort: Option<String> },
    EvidenceId { parent: SessionScope, id: EvidenceId },
    // codex variants in Task 3
}
pub enum EvidenceId { Uuid(String), ConfigDir(PathBuf) }
pub fn extract_claude_line(scope: &SessionScope, line: &str) -> Vec<LogFact>;
pub fn extract_meta_json(parent: &str, agent_id: &str, bytes: &[u8]) -> Option<LogFact>;
pub fn scan_raw_ids(line: &str) -> Vec<EvidenceId>;           // carve-in 1: regex only, no JSON parse
pub fn sanitize_command_script(script: &str) -> String;       // strips ^\w+=\S+ prefixes, 60 chars
pub fn repo_relative(path: &str, cwd: &str) -> String;        // accepts plain paths and file:// URIs
```
Typed envelopes: per-record serde structs holding ONLY allowlisted
fields (`#[derive(Deserialize)]` with no catch-all body fields), matching
`src/provider/claude.rs:12-30`. `Usage.sample_id` = `message.id` for
dedup; `effort` from the verified top-level assistant field.
Task-notification status via carve-in 2 (regex over `content`, extracting
task-id + status only; `failed` → `SubagentEnded { failed: true }`).

- [ ] **Step 1: Failing tests:**
  `meta_json_yields_role_and_subject`;
  `ai_title_latest_is_file_order`;
  `bash_description_becomes_activity_line`;
  `edit_paths_render_repo_relative`;
  `usage_dedupes_by_message_id_and_sums_output_only` (fixture repeats a
  message.id 3×; expected total counts it once; cache fields never enter);
  `effort_extracted_from_assistant_records`;
  `task_notification_failed_maps_to_failed`;
  `typed_envelopes_cannot_hold_bodies` (compile-time shape: the envelope
  structs have no field capturing message content; asserted by
  deserializing a body-bearing fixture record and checking the struct's
  Debug output length is bounded);
  `raw_id_scan_finds_uuids_and_config_dirs_without_json_parse`;
  `unknown_record_types_skip_silently`.
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(provider): typed allowlist extraction for Claude transcripts`.

### Task 3: Typed codex extraction

**Files:**
- Create: `src/provider/codex_facts.rs`
- Modify: `src/provider/facts.rs`, `src/provider/mod.rs`
- Test: unit tests

**Interfaces (adds):**
```rust
LogFact::CodexMeta { rollout_id: String, cwd: String /* may be file:// */, originator: String, internal: Option<CodexInternal>, cli_version: String },
LogFact::CodexTurn { rollout_id: String, turn_id: String, model: String, effort: Option<String>, sandbox: Option<String> },
LogFact::CodexTurnStarted { rollout_id: String, at_ms: i64 },
LogFact::CodexTurnComplete { rollout_id: String, at_ms: i64 },
LogFact::CodexTurnAborted { rollout_id: String, at_ms: i64 },
LogFact::CodexPid { rollout_id: String, pid: u32 },           // parsed from JSON STRING
pub enum CodexInternal { Named { name: String }, ThreadSpawn { parent_thread_id: String, nickname: Option<String>, role: Option<String> } }
pub fn extract_codex_line(rollout_id: &str, record_ordinal: u64, line: &str) -> Vec<LogFact>;
```
Usage for codex: per-turn deltas from `last_token_usage.output_tokens`
with `sample_id` = the caller-maintained record ordinal; turns close
at `CodexTurnComplete`. Activity: commentary at `item.content[0].text`
(phase == "commentary") preferred; else CommandExecution — sanitize the
SCRIPT ELEMENT of the argv array. `session_meta` records surface as facts
in file order; the Task 5 consumer retains the FIRST as identity
(first-wins enforcement and its test are a Task 5 obligation).

- [ ] **Step 1: Failing tests:**
  `session_meta_records_surface_in_file_order`;
  `turn_context_per_turn_model_and_effort` (resume fixture: two differing
  contexts both surface with their turn ids);
  `commentary_preferred_then_sanitized_argv_script`;
  `pid_parses_from_json_string`;
  `usage_is_turn_delta_not_total_sum` (resume fixture: expected = sum of
  every per-record `last_token_usage.output_tokens` — 120+80+250 = 450;
  NOT sum of totals (570), NOT max or last (250));
  `internal_subagent_both_shapes`;
  `turn_aborted_maps_to_cancelled_fact`;
  `file_uri_cwd_relativizes`;
  `bodies_never_enter_facts` (stdout/stderr/reasoning from the fixture
  appear in no fact and no envelope field).
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(provider): typed allowlist extraction for codex rollouts`.

### Task 4: Worker extension — admission, tailing, id evidence

**Files:**
- Create: `src/provider/lane.rs` (admission + evidence graph only)
- Modify: `src/provider/mod.rs`, `src/provider/claude.rs` (route
  per-line extraction through the EXISTING tail pipeline; add codex
  rollout paths and per-session subagent dirs to the existing worker's
  target handling — no second watcher/offset store)
- Modify: `src/main.rs` (backfill window env)
- Test: `lane.rs` unit tests with tempdir trees; boundary instrumentation

**Interfaces (produces):**
```rust
pub struct Admission { /* admitted scopes, derived roots, anchor */ }
impl Admission {
    pub fn new(anchor_ms: i64) -> Self;
    pub fn admit_pane_session(&mut self, provider: Provider, session_id: &str);
    pub fn on_evidence(&mut self, parent: &SessionScope, id: &EvidenceId, discovered: &AdmissionIndex) -> Option<SessionScope>;
    pub fn is_admitted_path(&self, path: &Path) -> bool;      // consulted BEFORE open
    pub fn is_admitted_file(&self, path: &Path, modified_ms: i64) -> bool;
}
pub fn backfill_anchor_ms(earliest_db_event: Option<i64>, now_ms: i64, window_ms: i64) -> i64; // max(earliest, now - window)
```
`is_admitted_path` excludes `tool-results/` categorically. The worker
opens a file ONLY after admission — instrumented via a test hook counting
open attempts (feature-gated counter alongside the existing provider
diagnostics counters).

- [ ] **Step 1: Failing tests:**
  `open_admitted_regular_file_gates_strangers_and_tool_results` (stranger session + tool-results
  dir in the tempdir; open-counter for them stays 0 — not merely zero
  facts);
  `subagents_dir_admitted_via_parent`;
  `evidence_uuid_admits_matching_rollout_only` (a uuid with no matching
  discovered artifact admits nothing);
  `config_dir_evidence_creates_scoped_root`;
  `anchor_is_max_of_db_event_and_window`;
  `date_shard_scan_bounded_by_anchor`;
  `offsets_advance_incrementally_via_existing_tail`.
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(provider): evidence-gated admission inside the existing worker`.

### Task 5: Synthesis — keys, deterministic ids, event mapping

**Files:**
- Modify: `src/provider/lane.rs` (synthesis), `src/provider/mod.rs`
  (`ProviderEvent::Synthesized(ControllerEvent)`,
  `ProviderEvent::RunLiveness { key, at_ms }`,
  `ProviderEvent::Telemetry { key, output_tokens, model, effort }`),
  `src/herdr/collector.rs` (route existing-worker `bootstrap_file` and
  `TailFile` opens through `is_admitted_file` / `open_admitted_regular_file`;
  route Synthesized through the existing controller-event application;
  Telemetry/Liveness to Task 6/7 entries)
- Test: lane + collector unit tests

**Interfaces:**
- Key recipes (pinned against `src/hook_adapter.rs` constants):
  `hook:claude-code:<uuid>` / `hook:claude-code:<uuid>:agent:<agentId>`;
  codex native claim `RunKey::Native { Codex, rollout_id }`.
- Event ids:
  `log:<artifact-basename>:<record-ordinal>:<kind-slug>:<target-id>`; the id
  keys on (artifact basename, record ordinal, kind, target identity) and is
  order-independent, letting the durable event ledger dedupe replays across
  restarts.
- Mapping: SubagentAppeared → Dispatch + TaskStarted (label =
  description, agent type in metadata); SubagentEnded → Complete/Failed;
  codex admission-with-evidence → Dispatch; CodexTurnComplete →
  grace-deferred Complete (Task 6); CodexTurnAborted → Cancelled.

- [ ] **Step 1: Failing tests:**
  `synthesized_claude_keys_byte_match_hook_adapter` (drive the same
  fixture identity through `hook_adapter` and the lane; assert equal
  keys — the convergence guarantee);
  `event_ids_deterministic_across_replay` (extract twice; ids identical;
  reducer ledger dedupes to one apply);
  `event_ids_never_prov_prefixed`;
  `dispatch_and_started_carry_subject_and_kind`;
  `failed_notification_yields_failed_state`;
  `hook_and_lane_same_fixture_yield_single_run` (apply hook-adapter
  events AND lane events for the same identity; exactly one run, no
  merge conflict).
- [ ] **Step 2–4:** Wire every collector-reached `bootstrap_file` and
  `TailFile` open through the provider admission seam before opening the
  descriptor, making the Task 4 guarantee enforced; fail → implement →
  gates (collector routing must not change the reducer-owning select set).
- [ ] **Step 5:** Commit `feat(lane): synthesize controller events with hook-identical identity`.

### Task 6: Lifecycle state — grace, reopen, inactivity, liveness

**Files:**
- Modify: `src/provider/lane.rs` (grace/inactivity timers),
  `src/reducer.rs` (liveness touch entry; lane-close honoring guards;
  reopen gate), `src/store/mod.rs` (terminal-provenance read-model:
  `pub fn terminal_event_sources(&self) -> Result<HashMap<RunId, String>>`
  querying the persisted `events` rows for the latest terminal event per
  run), `src/main.rs` (grace/inactivity envs)
- Test: reducer + lane tests

**Interfaces:**
```rust
// reducer
pub fn touch_run_liveness(&mut self, key: &RunKey, at_ms: i64) -> Vec<PersistOp>; // dismissed checked BEFORE touch; non-terminal only
pub fn apply_lane_close(&mut self, key: &RunKey, at_ms: i64) -> Vec<PersistOp>;   // -> EndedUnknown; never on terminal/dismissed; bypasses the has_controller_task_state_event early-return deliberately (that flag is set by synthesized events too)
// reopen: TaskStarted on terminal accepted IFF metadata.source == SOURCE_LOG_LANE AND provenance(run) == SOURCE_LOG_LANE (from terminal_event_sources at restore, maintained live thereafter)
```

- [ ] **Step 1: Failing tests:**
  `complete_lands_after_grace_only`;
  `resume_within_grace_never_flaps`;
  `reopen_only_for_lane_provenance` (hook-terminal run + lane TaskStarted
  → rejected; lane-terminal + lane TaskStarted → reopened);
  `reopen_provenance_survives_restart` (persist, restore via
  terminal_event_sources, reopen still gated correctly);
  `inactivity_closes_lane_created_runs` (synthesized run with
  has_controller_task_state_event=true reaches EndedUnknown — pins the
  B3 fix);
  `lane_close_never_touches_terminal_or_dismissed`;
  `liveness_touch_checks_dismissal_first`;
  `root_liveness_defers_hook_only_expiry` (incident regression).
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(lane): grace, provenance-gated reopen, inactivity close, liveness`.

### Task 7: Telemetry map and token aggregation

**Files:**
- Modify: `src/model/entities.rs` (serde-skipped
  `pub telemetry: RunTelemetryMap` on the model:
  `HashMap<RunId, RunTelemetry { output_tokens: u64, started_wall_ms: i64, model: Option<String>, effort: Option<String>, per_turn: Vec<TurnAttr> }>`),
  `src/reducer.rs` (apply `ProviderEvent::Telemetry`),
  `src/herdr/collector.rs` (routing)
- Test: reducer unit tests

- [ ] **Step 1: Failing tests:**
  `telemetry_accumulates_deduped_output_tokens`;
  `telemetry_survives_backfill_replay_identically` (same fixture replayed
  after simulated restart reproduces identical totals via ledger-deduped
  events + recomputed telemetry);
  `telemetry_is_not_persisted` (serialize the model snapshot; assert no
  telemetry bytes);
  `per_turn_model_effort_latest_wins_for_display`.
- [ ] **Step 2–4:** fail → implement → gates (publication goes through
  the existing model watch; no new channel; paint-separation test stays
  green).
- [ ] **Step 5:** Commit `feat(model): transient run telemetry from log facts`.

### Task 8: Placement — dispatch-parent nesting with retention rules

**Files:**
- Modify: `src/tui/view.rs` (`place_runs` — function begins at `:1138`;
  the fallback chain at `:1174-1183`), `src/tui/projection.rs` (nested
  run rows)
- Test: view/projection tests

Order: live-exec pane → latest ended-exec pane (EXISTING behavior — the
pin at `src/tui/view.rs:3019` stays green) → dispatch parent → Unattached.
Hidden parent → children to Unattached for that frame; cycle bail.

- [ ] **Step 1: Failing tests:**
  `headless_child_nests_under_dispatch_parent`;
  `ended_exec_pane_fallback_still_wins_over_parent` (pins the retained
  behavior explicitly);
  `three_level_chain_renders`;
  `hidden_parent_children_fall_to_unattached`;
  `cycle_bails_to_unattached`;
  `sibling_order_is_dispatch_order`.
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(tui): dispatch-parent nesting for execution-less runs`.

### Task 9: Row content — glyphs, subjects, live line, stall, ghost

**Files:**
- Modify: `src/tui/view.rs` (row builder `:1489` region),
  `src/tui/projection.rs` (stall with descendant suppression),
  `src/activity.rs` (ghost window), `src/main.rs` (stall/ghost envs)
- Test: view/projection tests

- [ ] **Step 1: Failing tests:**
  `glyph_reflects_state_and_stall`;
  `stall_suppressed_while_descendant_active`;
  `subject_chain_meta_title_cwd_id`;
  `codex_worker_rows_have_no_subject`;
  `live_prefers_commentary_then_command`;
  `terminal_rows_drop_live`;
  `ghost_provisional_short_window`.
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(tui): human-first rows with stall and ghost handling`.

### Task 10: Columns, shedding, filter indicator, header

**Files:**
- Modify: `src/tui/view.rs` (column renderer; `footer_line` `:443` gains
  a state parameter — call site `:144`; draft editor at `:479` wins while
  active; `header_line` `:798` up:-shrink), `src/tui/projection.rs`
  (cells), `src/tui/app.rs` (no key changes; state plumb-through)
- Test: view tests

- [ ] **Step 1: Failing tests:**
  `columns_shed_at_declared_thresholds`;
  `model_names_shorten_and_ellipsize`;
  `tok_output_only_and_mean_stable_across_completion`;
  `committed_filter_indicator_persists_and_draft_overrides`;
  `up_field_shrinks_and_drops_last` (enumerate every previously-pinned
  never-drop expectation this changes);
  `deep_indent_compresses_when_narrow`.
- [ ] **Step 2–4:** fail → implement → gates (paint-separation
  non-vacuous).
- [ ] **Step 5:** Commit `feat(tui): metric columns, responsive shedding, visible filters`.

### Task 11: Summary, Detail, doctor, diagnostics plumbing

**Files:**
- Modify: `src/tui/projection.rs` + `src/tui/view.rs` (two Summary
  tables; scope follows selection; `w` toggle in `src/tui/app.rs`
  overlay region `:964`; Detail: effort/sandbox/evidence-paths/per-turn
  history/token breakdown), `src/diagnostics/mod.rs` (lane counters into
  `RuntimeDiagnosticsSnapshot` beside `enrichment_counters`),
  `src/doctor.rs` (three checks), `src/herdr/collector.rs` (structural
  six-selects test lives here)
- Test: projection/doctor/collector tests

- [ ] **Step 1: Failing tests:**
  `summary_tables_driven_by_real_extraction` (totals come from applying
  the fixtures end-to-end, not hand-built);
  `summary_scope_follows_selection_w_toggles`;
  `detail_shows_effort_sandbox_evidence_and_breakdown`;
  `doctor_warns_on_unreadable_roots`;
  `doctor_coverage_counts_pane_sessions_with_artifacts`;
  `doctor_freshness_uses_watcher_observations_not_mtime` (kill the
  watcher in the harness; freshness goes stale while mtimes advance);
  `exactly_six_reducer_owning_selects` (structural pin).
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `feat(tui,doctor): summary scoping, detail depth, lane health`.

### Task 12: Shipped-defect fixes

**Files:**
- Modify: `src/reducer.rs` (`reconcile_gap_inner` begins `:773`; ULID
  mint at `:829-830` — reuse keyed on (pane, terminal, resolved
  occupant); skip re-persisting terminal executions),
  `src/herdr/collector.rs` (`probe_topology_matches_model` `:2203` —
  exclude volatile agent/execution state)
- Test: reducer/collector tests

- [ ] **Step 1: Failing tests:**
  `reconnect_reuses_gap_execution_same_occupant`;
  `occupant_change_mints_new_execution` (upsert ownership rewrite at
  `src/store/mod.rs:1778-1782` makes this mandatory);
  `terminal_executions_not_repersisted` (pin PersistOp counts across two
  reconciliations);
  `probe_ignores_volatile_state_but_detects_topology_change`.
- [ ] **Step 2–4:** fail → implement → gates.
- [ ] **Step 5:** Commit `fix(reducer,watchdog): bounded gap executions, non-volatile probe`.

### Task 13: Documentation + live smoke record

**Files:**
- Modify: `README.md`, `docs/guides/controller-emit-setup.md`,
  `docs/tui.md`, `docs/cli.md`, `docs/design/herdr-top-mvp.md` (per spec
  §11: hook = optional precision; lineage coverage boundary + the
  echo-the-session-id convention; TOK = output tokens; new env vars;
  update the README art for glyphs/columns and re-verify widths)
- Create: `docs/internal/superpowers/verification/2026-increment-9-smoke.md`
  (the six-point live-demo record, executed against this repository's own
  workflow before publication)
- Test: link/width checks; no cargo change

- [ ] **Step 1:** Docs rewrite per spec §11 with code-accurate claims.
- [ ] **Step 2:** Execute and record the six-point smoke (zero-config
  startup, tree with roles/subjects, live lines, lifecycle incl.
  ended_unknown, Summary real numbers, dismissal/restart persistence).
- [ ] **Step 3:** Commit `docs: log-lane era documentation and smoke record`.

---

## Self-review (v2)

- Every blocker mapped: B1→§1 coverage honesty + Task 4/5 evidence
  mechanism; B2→Task 5 key/id recipes + convergence tests; B3→Task 6
  `apply_lane_close` + inactivity test; B4→Task 6 provenance read-model;
  B5→Tasks 2/3/7 token definitions + tests; B6→Task 7 telemetry map +
  Task 11 diagnostics files; B7→typed envelopes + carve-ins +
  body-bearing fixtures + open-counter tests + tool-results exclusion;
  B8→Task 4 extends the existing worker. Should-fix S1–S9 and C1–C3
  folded (anchors corrected; splits done; smoke owned by Task 13;
  vacuous tests replaced).
- Placeholder scan: none. Type consistency: `LogFact`/`SessionScope`/
  `EvidenceId` defined Task 2, extended Task 3, consumed Tasks 4–7;
  telemetry types defined Task 7, consumed Tasks 10–11; env names match
  Global Constraints everywhere.
