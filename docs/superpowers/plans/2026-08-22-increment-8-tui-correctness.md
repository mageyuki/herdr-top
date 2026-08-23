# Increment 8: TUI Correctness and Readability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make herdr-top's execution model self-healing (liveness watchdog, protocol-20 frame tolerance, safe native-session resolution, hook-run lifecycle) and make its TUI legible (readable task rows, tree glyphs, names, timing metrics, summary overlay, clear key).

**Architecture:** All changes stay inside the existing single-crate pipeline: collector (socket ingest, owns the reducer input) → reducer (domain model + persistence ops) → store (SQLite, schema_migrations-versioned) → tui (watch-channel read model). New state is additive nullable columns and additive `#[serde(default)]` struct fields; new UI builds on the existing overlay, projection, and footer mechanisms; the one new channel is a TUI→collector operator-command path (Task 8).

**Tech Stack:** Rust (edition 2024, rust-version 1.97.1), ratatui 0.30, tokio, rusqlite, serde.

**Spec:** `docs/superpowers/specs/2026-08-22-increment-8-tui-correctness-design.md` (user-approved 2026-08-22; SessionEnd auto-dismiss decision 2026-08-22). Diagnosis evidence: `~/.research/mageyuki--herdr-top/herdr-top-ui-fixes/diagnosis-attach-bugs.md`.

**Revision:** v2. The pre-implementation plan review (archived at `.superpowers/sdd/increment8-plan-review.md`, verdict REVISE: 9 blockers / 10 should-fix / 8 consider) is fully incorporated; every finding was fixed at source in this version, and the SessionEnd lifecycle was re-decided by the user as auto-dismiss (B6).

## Global Constraints

- Implementation base: post-Increment-7 `main`. Single implementation branch, dedicated linked git worktree.
- CI-exact gate for every task: `PATH="$HOME/.cargo/bin:$PATH" cargo fmt --all -- --check && PATH="$HOME/.cargo/bin:$PATH" cargo clippy --locked --all-targets --all-features -- -D warnings && PATH="$HOME/.cargo/bin:$PATH" cargo test --locked --all-targets --all-features && PATH="$HOME/.cargo/bin:$PATH" cargo test --locked --doc`.
- SIGHUP sensitivity: `runner_fixture_reaps_timeout_and_signal_groups` and `orchestration_signal_traps_are_self_contained_across_reexec` fail under inherited SIGHUP=SIG_IGN. Never `nohup`; bare `setsid` does not reset an ignored disposition — run gates via `setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- cargo test ...` when in doubt.
- No new tokio features: the watchdog uses an injected clock; do NOT add `test-util` to Cargo.toml. No new dependencies anywhere in this increment.
- Timestamp discipline: model bookkeeping (`created_at_ms` / `updated_at_ms` / `finished_at_ms` / `dismissed_at_ms`) uses **receipt time** (`receipt_time_ms`), never the producer-supplied event timestamp (`EventMetadata.timestamp_ms` is documented as forbidden for ordering/retention at `src/model/entities.rs:805`).
- Privacy rules unchanged: no prompt/response excerpts; controller labels and captured tab/pane names pass through `sanitize_controller_text` (256-byte cap, `src/model/entities.rs:872-875`); diagnostics counters carry no pane identifiers or free text. Token metrics and free-text activity excerpts are Increment 9.
- Never use nesting to represent dependencies in the tree (design doc §6.2/§6.3).
- Schema: this increment bumps `CURRENT_SCHEMA_VERSION` 4 → 5 exactly once (Task 1 owns the bump and the whole V5 delta; later tasks add NO further schema changes). Downgrade is unsupported: a pre-Increment-8 binary refuses a V5 database (`src/store/schema.rs:159-168`) — Task 9 documents this.
- Implementers do not push, merge, or rebase. Wrapper commits to the assigned branch only after independent verification.

---

### Task 1: Timing, subject, dismissal, and name columns (store + model + reducer bookkeeping)

**Files:**
- Modify: `src/model/entities.rs` (TaskRun ~line 41; Tab/Pane entities; every construction site `cargo check` reports)
- Modify: `src/store/schema.rs` (add the V5 migration delta to the `schema_migrations` chain; bump `CURRENT_SCHEMA_VERSION` 4 → 5; do NOT touch the frozen `SCHEMA_V1` — `migrate()` re-runs it before deltas, so putting new columns in both places fails duplicate-column on fresh databases)
- Modify: `src/store/mod.rs` (`restore_task_runs` ~line 853; tab/pane restore; the task_runs upsert at lines 1686-1733 — NOT `src/store/writer.rs`, which contains no task_runs references)
- Modify: `src/reducer.rs` (timestamp/subject/dismissal bookkeeping at ALL run-mutation sites)
- Test: `src/store/mod.rs` and `src/reducer.rs` mod tests

**Interfaces:**
- Produces on `TaskRun` (all `#[serde(default)]`): `created_at_ms: Option<i64>`, `updated_at_ms: Option<i64>`, `finished_at_ms: Option<i64>`, `subject: Option<String>`, `dismissed_at_ms: Option<i64>`.
- Produces on the tab entity: `label: Option<String>`; on the pane entity: `display_name: Option<String>` (both `#[serde(default)]`, both sanitized at capture time by Task 6).
- Produces reducer bookkeeping contract (clock = receipt time): `created_at_ms` set at run construction; `updated_at_ms` advanced by every mutation of the run; `finished_at_ms` set once on first terminal transition; `dismissed_at_ms` cleared by any non-terminal mutation; `subject` = first non-empty sanitized controller label, never overwritten by `None`.
- V5 delta (single migration, owned here): `ALTER TABLE task_runs ADD COLUMN subject TEXT; ALTER TABLE task_runs ADD COLUMN dismissed_at_ms INTEGER; ALTER TABLE tabs ADD COLUMN label TEXT; ALTER TABLE panes ADD COLUMN display_name TEXT;` (timing columns already exist — `src/store/schema.rs:495-497`).

- [ ] **Step 1: Extend the structs.** `TaskRun` gains the five fields above; the tab and pane entities gain their name fields. Fix every construction site `cargo check --locked --all-targets --all-features` reports, adding explicit `None`s (no `Default` shortcut).

- [ ] **Step 2: Failing migration tests.** In `src/store/mod.rs` tests: (a) build a V4 database inside the test by applying the V1 base plus the V2..V4 deltas from the migration chain (reuse the chain's own SQL — do not hand-copy DDL), stamp `schema_migrations` at 4, then open through the normal path and assert migration to 5 succeeds and restore yields `None` for all new fields; (b) fresh-database open (full chain) succeeds — this is the duplicate-column guard; (c) round-trip a run with all five fields populated plus a tab label and pane display_name.

- [ ] **Step 3: Implement the V5 delta + restore + upsert.** Add the delta and version bump; extend `restore_task_runs` (`src/store/mod.rs:853`) and the tab/pane restore to read the new columns; extend the task_runs upsert (`src/store/mod.rs:1686-1733`) and tab/pane upserts to write them. The existing `ON CONFLICT` keeps `created_at_ms = MIN(task_runs.created_at_ms, excluded.created_at_ms)` (earliest-observed) — leave that semantics; the regression test must use an EARLIER second timestamp and assert the earlier value wins (a later-second-timestamp test cannot distinguish MIN from first-write).

- [ ] **Step 4: Run the store tests.** Expect PASS.

- [ ] **Step 5: Failing reducer tests.** (a) run construction stamps `created_at_ms` from receipt time (construction site `src/reducer.rs:950-956` currently sets no timestamps); (b) every mutation site advances `updated_at_ms` — cover the controller path AND the non-controller mutation sites at `src/reducer.rs:998`, `:1021`, `:1567`, `:1586`, `:1715` (write one parameterized test per site family); (c) first terminal transition sets `finished_at_ms`, a duplicate terminal event does not move it; (d) a controller `progress` with `label: Some("Map hook payloads")` sets `subject`, later `None` labels leave it; (e) a non-terminal mutation on a run with `dismissed_at_ms: Some(t)` clears it.

- [ ] **Step 6: Implement the bookkeeping.** Thread receipt time into the mutation helpers; keep `persist_task_run` (`src/reducer.rs:1751`) passing the fields through `PersistTaskRun`.

- [ ] **Step 7: Full gate; commit** `feat(model): carry timing, subject, dismissal, and name state through store and reducer`.

---

### Task 2: Native-session resolution hardening (before the watchdog lands)

**Files:**
- Modify: `src/reducer.rs` (`run_for_native_session`, lines 1727-1749)
- Test: `src/reducer.rs` mod tests

**Interfaces:**
- Consumes: Task 1 fields (compiles against them; no behavioral coupling).
- Produces: resolution contract — a native sid resolves to an existing run whenever ANY unambiguous evidence binds it: key alias (already handled — `task_run_by_key` consults `run_ids_by_key`, which restore re-registers at `src/store/mod.rs:914-921`), or agent-node evidence regardless of the owning run's key kind. Snapshot application never mints a duplicate for a bound sid.

- [ ] **Step 1: Pin the already-good paths.** Two green-first tests documenting current behavior so regressions surface: (a) a controller-keyed run with a restored native-binding alias resolves via `task_run_by_key`; (b) a `RunKey::Native`-keyed run resolves directly. (These pin the paths the plan review proved already work — they are documentation tests, not the fix.)

- [ ] **Step 2: Failing test for the real gap.** The candidate filter at `src/reducer.rs:1742-1748` restricts agent-node-derived resolution to runs whose key is `Controller|Native` and requires unanimity. Construct: a `RunKey::NativePath`-keyed run owning an agent node whose `native_session_id` is the probe sid, no alias registered. Assert `run_for_native_session` resolves to that run (currently the filter drops it → snapshot application would mint a duplicate → the unique index `task_runs_native_session_binding_unique` rejects the persist).

- [ ] **Step 3: Implement.** Widen the filter to accept `NativePath`-keyed owners (keep `Provisional` excluded — a provisional key is not identity evidence), keep the unanimity requirement across distinct runs, and add a second failing-then-passing test: two DIFFERENT runs each with agent nodes claiming the sid → `None` (ambiguity stays unresolved rather than guessed).

- [ ] **Step 4: Full gate; commit** `fix(reducer): resolve native sessions through native-path agent evidence without minting duplicates`.

---

### Task 3: Collector liveness watchdog, reconnect backoff, and flat pane_agent_detected

**Files:**
- Modify: `src/herdr/collector.rs` (the primary event reader `spawn_event_reader`, fn at ~2569 with `stream.next_event()` at ~2581 — NOT `run_enrichment_reader`; the reconnect supervision that currently uses the flat 50 ms `RECONNECT_DELAY` at ~line 66; the two `pane_agent_detected` arms at ~3912 and ~5203-5205; the resync-admission gate at ~2060; the gap machinery — `GapKind::{Startup,Reconnect,SocketReplacement}` already exist, reuse `Reconnect`)
- Modify: `src/diagnostics/` (one new COUNTER for flat pane_agent_detected frames — the diagnostics module exposes counters/snapshots, not free-text observations; the counter carries no pane identifiers)
- Test: `src/herdr/collector.rs` mod tests (frames inline as `serde_json::json!` values or via the `fixture_payloads` format — note `tests/common/mod.rs:166-168` requires `conn`/`dir`/`payload` wrapper lines, so a bare frame file is silently empty; prefer inline frames in mod tests)

**Interfaces:**
- Consumes: Task 2's resolution contract (snapshot reconciliation after a watchdog reconnect exercises it).
- Produces: `LivenessPolicy { timeout_ms: i64 }` default `30_000`, injected through the collector constructor; pure helpers `fn silence_deadline(last_event_at_ms: i64, policy: &LivenessPolicy) -> i64` and `fn backoff_delay_ms(consecutive_failures: u32) -> u64` (1s, 2s, 4s ... capped 60_000; resets to zero on the first event after a successful reconnect).

- [ ] **Step 1: Failing pure-logic tests.** Unit-test `silence_deadline` and `backoff_delay_ms` exhaustively (0 failures → 1s; 6 failures → 60s cap; reset semantics via a small state struct). No tokio time manipulation anywhere — these are pure functions (the spec's injected-clock requirement; do NOT add tokio `test-util`).

- [ ] **Step 2: Implement the watchdog in `spawn_event_reader`.** Add a deadline arm to the reader's existing select, PRESERVING the existing `cancellation.cancelled()` and `targets.changed()` branches:

```rust
tokio::select! {
    _ = cancellation.cancelled() => break,
    _ = targets.changed() => { /* existing branch body unchanged */ }
    event = stream.next_event() => {
        last_event_at_ms = clock_now_ms();
        /* existing event handling unchanged */
    }
    () = tokio::time::sleep(remaining_until(silence_deadline(last_event_at_ms, &policy))) => {
        // Silent-but-open stream: exit the reader with a distinguished
        // WatchdogSilence reason so the supervisor reconnects.
        break;
    }
}
```

The supervisor (the code that today re-spawns the reader after stream end with the flat 50 ms `RECONNECT_DELAY`) distinguishes watchdog exits: it records a `GapKind::Reconnect` gap, waits `backoff_delay_ms(failures)`, re-subscribes, requests a full snapshot, and reconciles through the existing observation-gap path (executions never survive a gap). Ordinary stream-end reconnects keep their current 50 ms behavior.

- [ ] **Step 3: Integration test with real (small) time.** Drive the reader with a scripted stream delivering one event then hanging forever; inject `LivenessPolicy { timeout_ms: 40 }`. Assert within a bounded wait (≤ 2 s): the reader exited with the watchdog reason, a `Reconnect` gap was recorded, the resubscribe hook ran, and after the hook supplies a fresh snapshot the model reflects it with prior executions retired (reuse the assertions of the existing startup-gap reconciliation tests). Real small timeouts, no clock mocking.

- [ ] **Step 4: Failing flat-frame tests, both arms.** Inline flat frame `{"event":"pane_agent_detected","data":{"agent":"claude","pane_id":"w1:p4","type":"pane_agent_detected","workspace_id":"w1"}}`: (a) main dispatch arm (~3912): not an error, flat counter incremented, `last_event_at_ms` stamped, no topology mutation; nested shape still runs `append_pane_upsert`. (b) resync-admission arm (~5203-5205, which gates admission at ~2060): flat shape is admitted as liveness rather than rejected — mirror the neighbouring arm's tolerance at 5206-5208.

- [ ] **Step 5: Implement both arms** (`data.get("pane")` absent + top-level `pane_id` present → count + liveness, no error, no upsert).

- [ ] **Step 6: Full gate; commit** `fix(collector): watchdog-reconnect silent streams and tolerate flat pane_agent_detected`.

---

### Task 4: Lifecycle — SessionEnd auto-dismiss, 24h hook-only expiry, and the time tick

**Files:**
- Modify: `src/hook_adapter.rs` (add the `"SessionEnd"` arm to `map_hook_payload`, match at lines 89-176)
- Modify: `src/herdr/controller.rs` (accept the new `dismiss` event_type on the envelope→normalized mapping)
- Modify: `src/reducer.rs` (apply `dismiss`: known run → set `dismissed_at_ms` from receipt time + persist; unknown run → true no-op, no placeholder)
- Modify: `src/activity.rs` (`is_default_visible_task_run`, line 63; new constant; dismissal check lands here too so Task 8 only wires the key)
- Modify: `src/tui/projection.rs` (real call site at line 146; `next_expiry_ms` currently schedules only terminal-visibility deadlines at 146-152 — extend for hook-only expiry deadlines) and `src/activity.rs:85` (the other real call site — `src/tui/view.rs` is NOT a call site)
- Modify: `src/tui/app.rs` + `src/main.rs` (a 1 Hz time tick that advances `state.now_ms` and marks the projection dirty — today `now_ms` only advances inside `recompute_projection`, so durations/expiry would freeze without it)
- Create: `docs/adr/2026-08-22-session-end-auto-dismiss.md`
- Modify: `docs/guides/controller-emit-setup.md` (mapping table; the SessionEnd rationale currently at lines ~256-258 is superseded)
- Test: hook_adapter, reducer, activity, projection mod tests

**Interfaces:**
- Consumes: Task 1's `dismissed_at_ms` / `updated_at_ms` and the clears-on-activity rule.
- Produces: `pub const HOOK_ONLY_STALE_VISIBILITY_MS: i64 = 24 * 60 * 60 * 1_000;` in `src/activity.rs`; new signature `is_default_visible_task_run(model: &DomainModel, run: &TaskRun, operator: &OperatorSnapshot, now_ms: i64) -> bool` implementing: dismissed ⇒ hidden; hook-only stale ⇒ hidden; else existing rules. "Hook-only" = `matches!(run.key, RunKey::Controller(_))` AND zero executions reference the run. Produces the app tick: `App::advance_clock(now_ms)` called at 1 Hz from the main loop.

- [ ] **Step 1: Failing hook test.** `"SessionEnd"` yields exactly one envelope: `event_type == "dismiss"`, `task_run_id == session_run_id`, `native_session_id == Some(session_id)`, no parent, no label; produced for BOTH providers (session lifecycle is not `supports_task_events`-gated). Implementation:

```rust
"SessionEnd" => vec![make_envelope(
    "dismiss",
    session_run_id,
    None,
    None,
    Some(payload.session_id.clone()),
    "session",
    "ended",
)],
```

- [ ] **Step 2: Failing controller/reducer tests.** (a) a `dismiss` envelope for a known run sets `dismissed_at_ms` (receipt time) and emits a persist op; the run's `state` is untouched (NOT terminal); (b) `dismiss` for an unknown run creates nothing (assert task-run count unchanged — the spec's edge case 4); (c) after a dismiss, a `task_started` for the same run clears the dismissal (Task 1's rule) — the resume path; (d) the existing stale-event protection tests still pass untouched (no terminal transition happens, so `reducer.rs:1891-1897` is never in play).

- [ ] **Step 3: Implement** the controller mapping + reducer application.

- [ ] **Step 4: Failing visibility tests.** With the new signature: (a) hook-only (controller-keyed, zero executions) non-terminal run with `updated_at_ms` older than 24h ⇒ hidden; fresh ⇒ visible; (b) a `Native`-keyed run of the same age ⇒ VISIBLE (the hook-only guard — this pins review finding B4); (c) a run with a live execution ⇒ visible regardless of age; (d) `dismissed_at_ms: Some(_)` ⇒ hidden even when fresh; (e) `updated_at_ms: None` (pre-Increment-8 restore) ⇒ never expiry-hidden; (f) terminal 1h rule unchanged.

- [ ] **Step 5: Implement visibility + scheduling.** Update both real call sites (`src/activity.rs:85`, `src/tui/projection.rs:146`). Extend `next_expiry_ms` (projection.rs:146-152) to also take the minimum over hook-only runs' `updated_at + 24h` deadlines and dismissal has no deadline (permanent until activity). Add the 1 Hz tick: `main.rs` loop calls `app.advance_clock(now)` which updates `state.now_ms` and triggers `recompute_projection` when a deadline passed or any live-duration row is visible; test `advance_clock` directly (pure call, no timers).

- [ ] **Step 6: ADR + guide.** ADR records: terminal mapping rejected because `reducer.rs:1891-1897` would make resumed sessions permanently invisible (the original no-op rationale, now superseded by dismiss); expiry-only rejected as too slow; auto-dismiss chosen (immediate cleanup, natural resume via clears-on-activity). Update the guide's mapping table and replace the stale SessionEnd rationale.

- [ ] **Step 7: Full gate; commit** `feat(lifecycle): auto-dismiss on SessionEnd, expire stale hook-only runs, tick the clock`.

---

### Task 5: Readable task-run rows and identity in the Detail overlay

**Files:**
- Modify: `src/tui/view.rs` (`task_run_label` line ~1152, `run_name` ~1182, `short_run_name` ~1200; new helpers)
- Modify: `src/tui/projection.rs` (Detail-overlay entity gains the identity block)
- Test: `src/tui/view.rs` mod tests

**Interfaces:**
- Consumes: Task 1's `subject` / timestamps; `AgentNode.{last_event_kind,last_tool_name,model_id,last_activity_at_ms}` (`src/model/entities.rs:60-65`).
- Produces: `worker_kind_label(run: &TaskRun) -> String`; `newest_agent_node<'a>(model: &'a DomainModel, run_id: RunId) -> Option<&'a AgentNode>` — deterministic: max by `(last_activity_at_ms, agent_node_id)` (the `agent_nodes` HashMap iterator is unordered, so an undefined "newest" would be nondeterministic); `run_row_label(model, run, now_ms) -> String` with the spec grammar (segments omitted when absent):

```
<worker-kind> <subject> — <activity> [model] [status] · <duration>
```

where `activity` = `format!("{event_kind}: {tool}")` when a tool name exists, else the event kind alone (live runs only) — e.g. `tool_use: Bash`.

- [ ] **Step 1: Failing row tests.** Table-driven with fixed timestamps: (a) controller-keyed `hook:claude-code:S:task:T`, subject "Implement I7 Task 2 wire tolerance", newest agent node `{last_event_kind: "tool_use", last_tool_name: "Bash", model_id: "gpt-5.6-sol"}`, live ⇒ exact string `claude-code Implement I7 Task 2 wire tolerance — tool_use: Bash [model:gpt-5.6-sol] [running] · 17m03s`; (b) no subject ⇒ key-derived fallback; (c) terminal ⇒ no activity segment, duration from `finished_at - created_at`; (d) missing timestamps ⇒ no duration segment; missing model ⇒ no `[model:...]`; (e) two agent nodes with different `last_activity_at_ms` ⇒ the newer one's activity/model wins, tied timestamps break by `agent_node_id`; (f) `[shared]` / `[dispatched by: ...]` / `[unlinked]` annotations preserved.

- [ ] **Step 2: Implement** the three helpers, rewire `task_run_label` (head = `run_row_label`, tail = existing annotations). `worker_kind_label`: `Native`/`NativePath` ⇒ `provider_label`; `Controller(name)` ⇒ the selector between `hook:` and the next `:` (whole name when the pattern is absent); `Provisional` ⇒ `provisional`. Duration format: `XhYYm` at ≥ 1h, `YYmZZs` at ≥ 1min, else `ZZs`.

- [ ] **Step 3: Detail identity block.** The run's Detail entity (`src/tui/projection.rs`) gains: full key text (old `run_name` output), `run_id`, bound native session id, created/updated/finished/dismissed timestamps. Test: detail lines contain the full `hook:claude-code:<sid>` text the row no longer shows.

- [ ] **Step 4: Full gate; commit** `feat(tui): human-readable task rows with identity moved to the detail overlay`.

---

### Task 6: Tree glyphs, tab/pane names end-to-end, DAG placeholder, footer tiers

**Files:**
- Modify: `src/tui/view.rs` (indentation at line 169 → connector glyphs; DAG render `render_dag` ~182; `footer_line` ~336 — it already has two tiers and its compact string is `q:stop Top; agents continue`, which is the mandated minimum)
- Modify: `src/tui/app.rs` + `src/main.rs` (read `HERDR_TOP_ASCII_TREE` once at startup, plumb a bool into the render state)
- Modify: `src/herdr/collector.rs` (capture names in BOTH ingest paths: live upsert `append_pane_upsert` ~4071 and tab handling; AND snapshot reconciliation, which currently drops names — snapshot `Tab` built from 2 fields at ~4537-4543, `PaneSnapshot` has no title at ~4555-4559 — otherwise every watchdog reconnect erases names)
- Test: view + collector mod tests

**Interfaces:**
- Consumes: Task 1's tab `label` / pane `display_name` fields; wire sources `PaneInfo.label` / `PaneInfo.terminal_title_stripped` (`src/herdr/types.rs:140-141`) and `TabInfo.label` (`:123`).
- Produces: name-resolution rule — pane display name = `label` if set else `terminal_title_stripped`; tab name = `label`; both sanitized via `sanitize_controller_text` at capture. Glyph rule — UTF-8 `├── └── │` by default, ASCII `|--` `` `-- `` `|` under `HERDR_TOP_ASCII_TREE=1`.

- [ ] **Step 1: Failing name-capture tests.** (a) live pane upsert with `label`/`terminal_title_stripped` stores the sanitized resolved name; (b) tab upsert stores `label`; (c) SNAPSHOT reconciliation carrying the same fields preserves names (this pins review finding S7 — extend the snapshot-side Tab/Pane construction); (d) a 300-byte name is truncated to the 256-byte cap.

- [ ] **Step 2: Implement capture** in both ingest paths + snapshot structs.

- [ ] **Step 3: Failing display tests.** `Pane: w1:p4 (UI修正)` when a name is known, `Pane: w1:p4` when not; same for tabs.

- [ ] **Step 4: Failing glyph tests.** Rows render connectors computed from the built row list (a row is a last child when no later row at its depth precedes the next shallower row): last child `└── `, others `├── `, ancestor continuation `│   `; ASCII variants under the flag. Fixture tree ≥ 3 deep, both modes asserted. `TreeRow.depth` semantics unchanged (projection/collapse untouched).

- [ ] **Step 5: Implement glyphs + flag plumbing** (env read once in `main.rs`/app construction — never per-frame).

- [ ] **Step 6: Failing DAG/footer tests.** (a) DAG view with zero dependency edges renders exactly one line `no dependency edges recorded` (the view-name-in-title requirement is ALREADY satisfied — `" Execution tree "` at view.rs:153, `" Dependency DAG "` at :185, pinned by the existing test at :2091 — do not re-add it); (b) footer: extend the existing two-tier `footer_line` to drop whole ` | `-separated hints from the right per width, minimum = the existing compact string `q:stop Top; agents continue`, never truncating mid-hint; Help overlay text unchanged.

- [ ] **Step 7: Implement; full gate; commit** `feat(tui): tree glyphs, durable tab and pane names, DAG placeholder, responsive footer`.

---

### Task 7: Session elapsed header and the Summary overlay (`s`)

**Files:**
- Modify: `src/tui/view.rs` (header line `host:... | session:... | LIVE`; new overlay render)
- Modify: `src/tui/app.rs` (key `s` in `handle_key` ~665-700; `Overlay` enum at line 258 gains `Summary`; overlay dismiss match at ~822-830; help text ~435)
- Test: app + view mod tests

**Interfaces:**
- Consumes: Task 1 timestamps; Task 4's tick (live refresh); Task 5's `worker_kind_label` + `newest_agent_node`.
- Produces: `summary_rows(model: &DomainModel, now_ms: i64) -> Vec<SummaryRow>`; `SummaryRow { worker_kind: String, model: String, run_count: usize, live_count: usize, total_duration_ms: i64, mean_duration_ms: Option<i64> }` — grouped by (worker-kind, model), sorted by `total_duration_ms` descending then keys; **total/mean cover terminal runs only** (spec §8.3); live runs count in `run_count`/`live_count` only. Token and tok/s columns render `-` (Increment 9 fills them).

- [ ] **Step 1: Failing aggregation test.** Two kinds × two models, mixed live/terminal, fixed timestamps: grouping, counts, terminal-only totals/means, sort order, timestamp-less runs contribute counts but no duration.

- [ ] **Step 2: Implement `summary_rows`.**

- [ ] **Step 3: Failing overlay/header tests.** `s` opens Summary, `s`/`Esc` closes (mirror the Help/Detail dismiss tests at app.rs:822-830); render shows one row per group plus a header row whose `tok`/`tok/s` columns are `-`; the top header line gains ` | up:HH:MM:SS` from `now_ms - session_start` where session_start = earliest `created_at_ms` in the model, else the app's own start instant; assert it changes when `advance_clock` moves `now_ms` (pinning review finding B5 for this surface).

- [ ] **Step 4: Implement; help text (`?` overlay) and footer hint list gain `s summary`; full gate; commit** `feat(tui): session elapsed header and role-by-model summary overlay`.

---

### Task 8: Clear key (`c`) over a new TUI→collector operator-command path

**Files:**
- Modify: `src/model/entities.rs` (or a small new module) — `pub enum OperatorCommand { DismissClearable }`
- Modify: `src/main.rs` (create the `tokio::sync::mpsc` channel; hand the `Sender<OperatorCommand>` to the App, the `Receiver` to the collector)
- Modify: `src/herdr/collector.rs` (a select arm on the command receiver in the task that owns the reducer — the invariant "no command-capable writer leaves the collector" at collector.rs:954 stays true: the command VALUE crosses the channel, the writer never does)
- Modify: `src/tui/app.rs` (key `c` sends `DismissClearable`; App holds only the Sender — its read-only relationship to the model is unchanged)
- Modify: `src/reducer.rs` (`apply_operator_command`: stamp `dismissed_at_ms` on every terminal run and every hook-only stale run, emit persist ops)
- Modify: `src/tui/view.rs` (footer/help hints gain `c clear`)
- Test: reducer + app mod tests

**Interfaces:**
- Consumes: Task 1's `dismissed_at_ms` + clears-on-activity; Task 4's visibility predicate and hook-only-stale definition (share the predicate — do not duplicate the 24h logic).
- Produces: `c` semantics — dismisses exactly: terminal runs, and hook-only stale runs per Task 4's predicate. Never deletes rows; dismissal survives restart (Task 1 column); new activity un-dismisses (Task 1 rule).

- [ ] **Step 1: Failing reducer test.** `apply_operator_command(DismissClearable, now)`: terminal runs and hook-only stale runs get `dismissed_at_ms: Some(now)` + persist ops; live attached runs untouched; already-dismissed runs unchanged (no duplicate persist).

- [ ] **Step 2: Implement `apply_operator_command`** reusing Task 4's hook-only-stale predicate.

- [ ] **Step 3: Failing plumbing test.** App-level: pressing `c` sends exactly one `DismissClearable` on the channel (App test with a capturing receiver); collector-level: a command on the receiver reaches `apply_operator_command` and the resulting model update propagates through the watch channel (integration-style mod test mirroring how existing collector tests drive the reducer).

- [ ] **Step 4: Implement the channel wiring** (`main.rs` → App sender / collector receiver select arm), footer/help hints.

- [ ] **Step 5: Restart round-trip test** (store): dismissed stays dismissed after restore. **Full gate; commit** `feat(tui): persistent clear over an operator-command channel`.

---

### Task 9: Design-doc updates

**Files:**
- Modify: `docs/design/herdr-top-mvp.md` — §6 (row grammar, glyphs, names, DAG placeholder), §7 (watchdog, backoff, flat-frame tolerance), §10 (new columns, restored fields, dismissal, schema v5 with downgrade-unsupported note), **§11** (keybindings `s`/`c`, footer tiers, Summary overlay — the UI requirement list lives at lines ~466-498, NOT §6), ADR list (add the session-end-auto-dismiss ADR)

**Interfaces:**
- Consumes: the as-landed behavior of Tasks 1-8 (write from the code, not from this plan).

- [ ] **Step 1: Update each section** in the document's existing voice: every keybinding, constant (30s watchdog, 60s backoff cap, 24h expiry, 256-byte name cap), and rule (dismissal never deletes; no dependency nesting; downgrade unsupported at schema v5) with final values.

- [ ] **Step 2: Cross-check** the ADR list, run the full gate, commit `docs: describe the increment 8 TUI correctness and readability behavior`.

---

## Sequencing

Serial 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9. Task 2 (resolution hardening) deliberately precedes Task 3 (watchdog) because watchdog-triggered snapshot reconciliation exercises the hardened lookup. File overlap makes parallel dispatch not worth the coordination (entities/store: 1, 6, 8; reducer: 1, 2, 4, 8; collector: 3, 6, 8; view: 5, 6, 7, 8; app/main: 4, 6, 7, 8); run serial.

## Phase R (increment close-out)

After Task 9: full CI-exact gate on the final HEAD under a SIGHUP-default shell; a live smoke run against the running herdr instance (current session's runs attach within one watchdog interval after a forced silence; rows show subjects; names shown; `s` and `c` work; SessionEnd of a scratch session dismisses its run and a resume restores it; restart preserves dismissals); then the standard publication flow (final whole-branch review before any push; push/PR on user instruction).

## Controller amendments

Where these entries conflict with the per-task text above, the shipped code is authoritative.

- **Task 4:** Lifecycle timestamp and dismissal bookkeeping landed on `TaskRun` in `src/model/entities.rs`, with clock control supplied at the production and workload-harness call sites. The proposed `App::advance_clock` interface did not land.
- **Task 4:** The default-visibility helper in `src/activity.rs` has the signature `is_default_visible_task_run(run, operator, runs_with_executions, now_ms)`, which supersedes the plan-declared `DomainModel` parameter.
- **Task 5:** The dependency-view integration landed in `src/tui/dag.rs`. A gated, wall-aligned refresh driven by cached deadlines in `src/tui/app.rs` and `src/tui/projection.rs` replaced the deleted `advance_clock` design.
- **Task 6:** `src/herdr/collector.rs` and `src/reducer.rs` retain captured names across observationally nameless reconciliation, while `tab_renamed` carries event authority: an explicit empty or absent label persists a clear rather than being treated as a nameless observation.
- **Task 6:** The watchdog probe's topology comparison follows the same name-retention rule as reconciliation; a probe-versus-model label mismatch therefore cannot make the watchdog oscillate permanently between divergence and reconnect.
- **Task 8:** The shared operator read model was extracted to `src/activity.rs`; collector command servicing is present across subscription setup, convergence, live, reconciling, and backoff lifecycles. The top-level `s` and `c` actions require empty modifiers, while Summary's overlay-local `s` close remains deliberately modifier-blind.
