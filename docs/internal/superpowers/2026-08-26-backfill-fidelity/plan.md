# Implementation plan: backfill fidelity (revised, round 3)

Spec: `docs/internal/superpowers/2026-08-26-backfill-fidelity/spec.md`
Branch: `agent/backfill-fidelity` (worktree, base `main` = 3d19995)

Order (revised per review): **Task 0 (G4 investigation) FIRST** — its findings
gate G2's grammar — then G2, G1(+G8 closure), G5, G7, G3+G6. Serial, one
branch/PR, one conventional commit per task (docs commit first, G4 notes as
its own docs commit). Every task starts with a VERIFY phase; if a task cannot
stay within its declared file set, stop and report for a Controller amendment.

## Task 0 — G4: inner-codex investigation notes (docs only, FIRST)

Expected files: `docs/internal/superpowers/2026-08-26-backfill-fidelity/inner-codex-notes.md`.
Read-only investigation of TWO real wrapper pairs: (i) an inner-codex pair
(a recent codex-implementer wrapper transcript from the user's Claude
config projects directory + its rollout under the Codex sessions
directory) and (ii) a headless-Claude pair (a `claude -p` wrapper, e.g.
claude-git-operator, + the child transcript). Answer for each: (a) identity
carried by the child artifact; (b) does the parent transcript reference it
in a G2-grammar position (spawn/resume command in tool-use `command`,
config-dir marker, or any typed/structural field — pin the actual wire
spelling and location of the child id) or only in prose? (c) a proposed
third-tier design sketch — for the inner-codex pair only. Finding (b)
GATES G2. STATUS: COMPLETED (commit 7d24295) — the gate FIRED TWICE:
first against the original typed-field position 4 (zero typed session
fields across 16,158 real tool-result records), then against the
spawn-correlated re-plan (both families carry the id only in spawn
stdout, which the codex wrapper backgrounds and log-redirects and the
claude wrapper truncates — neither real pair would have been admitted).
Final user-confirmed G2: positions 1-3 only; spawn-child linking
DEFERRED to a future increment designed from `inner-codex-notes.md`.
Pinned for that future work: Claude wire spelling is snake_case
`session_id` inside the spawn result's JSON output text.

## Task 1 — G2: typed lineage evidence grammar

Expected files: `src/provider/claude_facts.rs` (transient command-grammar
parsing with the privacy carve-in; extractor stops raw-line scanning),
`src/provider/codex_facts.rs` (same
restriction on its raw scan; structural `sub_agent_activity.agent_thread_id`
retained), `src/provider/facts.rs` (unconditionally: the `LogFact::EvidenceId`
doc comment describes the retired over-emitting behavior and must change;
scanner semantics stay a pure tokenizer), `src/provider/lane.rs` (only if
evidence admission call sites need signature changes),
`docs/design/herdr-top-mvp.md` (lineage paragraph),
`docs/guides/controller-emit-setup.md` (evidence description),
`docs/adr/2026-08-24-provider-log-allowlist.md` (read list, never-read
section, AND the "Pattern-extraction-only carve-ins" section: carve-in 1
— the license for the retired raw scan — is rewritten to the narrowed
command-text-only grammar and the "exactly three" count adjusted — spec
G2b'), `README.md` (the
"quoted report" lineage sentence), `tests/fixtures/provider-logs/MANIFEST.md`
(read-list mirror), `tests/provider_claude.rs`, `tests/provider_codex.rs`,
new fixtures under `tests/fixtures/provider-logs/` /
`tests/fixtures/provider/`. No spawn-result parsing and no tool-use/
tool-result correlation state anywhere: all admitted command evidence
(resume ids, config-dir markers) is same-line, so the stateless
single-line extraction in `claude_facts.rs` suffices unchanged in shape.

1. VERIFY: enumerate the raw-scan call sites (`claude_facts.rs` pre-parse
   scan; `codex_facts.rs` scan) and the FIVE filename-parsing production
   users of `scan_raw_ids` that must remain untouched (lane.rs ×4,
   collector.rs ×1). Enumerate the tests pinning current evidence (incl.
   `config_dir_evidence_is_preserved_from_bash_raw_line` and the three
   privacy tests `bash_without_description_uses_bare_tool_name`,
   `user_record_debug_excludes_message_and_tool_result_bodies`,
   `tool_use_result_debug_excludes_non_allowlisted_body_fields`) and the
   fixture line shapes.
2. Implement the closed grammar (spec G2a positions 1-3): meta.json
   (unchanged); command grammar parsed TRANSIENTLY from the tool-use
   `command` (codex exec resume <uuid> / claude --resume <uuid>, optional
   `CLAUDE_CONFIG_DIR=` prefix) under the privacy carve-in (spec G2b'),
   recognizing shell invocations, not substrings (a quoted/printed
   `codex exec resume ...` must not arm); codex
   structural child refs unchanged. General free-text scanning of record
   bodies is removed from evidence production. No spawn grammar and no
   result parsing (deferred increment).
3. Retarget `config_dir_evidence_is_preserved_from_bash_raw_line` to the
   command grammar; update the ADR (carve-in 1 rewrite included), both
   guides, README, and MANIFEST in the same commit; preserve all three
   privacy tests unretargeted.
4. Tests (red first): pasted-UUID fixture (directory-listing text inside a
   tool-result body) yields NO admission; quoted resume-lookalike inside a
   non-invocation command yields NO admission; resume-invocation fixture
   yields admission; config-dir fixture yields admission; codex structural
   ref yields admission; END-TO-END admission test that starts from
   only the pane root admitted (no hand pre-admission — the existing
   helpers pre-admit children and cannot prove production behavior).

## Task 2 — G1(+G8): fact-time stamping + generalized stale closure

Expected files: `src/herdr/collector.rs` (the `Synthesized` receipt-time
overwrite; meta.json read site threading `file.modified_ms`),
`src/provider/facts.rs` (`SubagentAppeared`/`SubagentEnded`/`EvidenceId`
gain `at_ms`), `src/provider/claude_facts.rs` (`extract_meta_json`
signature; SubagentEnded/EvidenceId record-timestamp threading),
`src/provider/codex_facts.rs` (its `LogFact::EvidenceId` construction
gains `at_ms`), `src/provider/lane.rs` (fact construction sites;
`fact_lifecycle_time` arms; SubagentEnded coalescing keeps its ordinal
rule and takes the EARLIEST timestamp), `src/reducer.rs` (stamping + the
new closure arms + tests), `src/model/entities.rs` (only if the
`EventMetadata` timestamp doc-contract wording must change — no field
changes expected), `src/activity.rs` (anchor helpers if shared).
Timestamp precedence per spec G1a: SubagentAppeared ← artifact
`modified_ms`; SubagentEnded/EvidenceId ← the parsed RECORD timestamp.

1. VERIFY: the exact overwrite (`collector.rs` `Synthesized` arm sets
   `receipt_time_ms = unix_now_ms()`), the stamping chain
   (`reducer.rs` `stamp_new_task_run` from `metadata.receipt_time_ms`),
   which facts lack timestamps and what their proxy does
   (`lane.rs` `fact_lifecycle_time`, `last_append_ms` fallbacks to 0), and
   what `file.modified_ms` is available at the meta.json read site.
2. G1a: stamp runs (and agent-node `last_activity_at_ms`) from fact time:
   use `metadata.timestamp_ms` where the fact carries it; thread real
   timestamps into the three timestamp-less fact types per the precedence
   in the preamble — SubagentAppeared ← artifact `modified_ms` at the
   meta.json read site; SubagentEnded/EvidenceId ← the parsed RECORD
   timestamp (Claude's record `timestamp` is optional: when absent, fall
   back to the artifact's `modified_ms`, never epoch; the Codex structural
   path carries `payload.occurred_at_ms`, not the top-level `timestamp` —
   use it). Receipt time remains in event rows. Monotonicity guard. Epoch-0
   guard: metadata seen before any parent append must not mint a 1970 run.
3. G1b: new SIBLING sweep arm (decided — not an extension: the queued arm
   uses `runs_with_executions`, which counts terminal executions and would
   silently exempt any run that ever had one, and stamps `now_ms`) for
   Controller-keyed `Running` runs — predicate: no LIVE (non-terminal)
   execution attached (the `apply_lane_close` liveness test) AND
   `!non_lane_task_state_runs.contains(run_id)` (the persisted
   hook-ownership protection; recency is NOT the guard) AND fact-time
   anchor older than `headless_inactivity_ms()`. Close anchored at LAST
   FACT TIME (pinned by test against the queued arm's `now_ms`).
4. G1c/G8: extend the same sweep to `Provisional`-keyed runs (no live
   execution, stale `updated_at.or(created_at)` anchor, both-None → never).
5. Tests (red first): fact-time display anchor; hours-old replay closes on
   next sweep; hook-owned Running root with restored ownership set, no
   execution, stale facts stays OPEN across restart; provisional closure +
   `c` dismissal; epoch-0 guard; monotonicity; closure anchor equals last
   fact time; reopen paths; `root_liveness_defers_hook_only_expiry`
   unmodified and passing.

## Task 3 — G5: ordering-safe usage attribution

Expected files: `src/provider/lane.rs` (usage dedup/attribution flow),
`src/reducer.rs` (telemetry application / pending-usage handling),
`src/herdr/collector.rs` (only if artifact-ordering is the chosen lever —
decide at VERIFY), fixtures (real-shape `.jsonl` + `.meta.json`), in-file
tests. `src/provider/claude_facts.rs` expected UNCHANGED (extraction is
already correct for both paths).

1. VERIFY: the loss mechanism end-to-end — lane inserts into
   `usage_samples` before the reducer can attribute; reducer returns empty
   for unknown runs; telemetry is transient and re-derived on restart
   backfill (pinned by `telemetry_survives_backfill_replay_identically`).
   Decide the mechanism between: (b) retain bounded unattributed usage
   keyed by scope and re-apply when the run appears, or (c) order artifact
   processing so run-minting facts precede usage. Option (a) — defer
   dedup-set insertion until attribution succeeds — is STRUCK: the reducer
   returns an empty Vec on both hit and miss, so the lane has no success
   feedback; choosing (a) would require a new feedback transport, which
   is out of scope. Record the decision and why.
2. Implement; preserve restart idempotence (no double counting) and fix the
   real hazard (zero-counting).
3. Tests (red first): fixture where usage precedes run-minting facts →
   metrics present (model/effort/tokens; duration NOT asserted); restart
   re-backfill → identical totals; late `.meta.json` cycle; out-of-window
   via lineage child stays `—`.

## Task 4 — G7: single unambiguous codex pane binding

Expected files: `src/herdr/collector.rs` (sessionless codex pane flow,
provisional target derivation), `src/provider/lane.rs` (expose rollout
creation time from the existing private filename parser; discovery
surface), `src/provider/mod.rs` (TargetSet if a new target form is needed),
`src/identity.rs` (binding plan for the promotion), `src/reducer.rs`
(heuristic binding application), in-file tests. Store
schema NOT expected; if the promotion cannot reuse the existing
provisional→native identity path without schema changes, STOP and report.

1. VERIFY: where provisional runs are minted for agent-bearing sessionless
   panes; what the identity machinery accepts for provisional→native
   promotion; what discovery exposes (`modified_ms` only today —
   creation time needs exposing from `rollout_filename_timestamp_ms`,
   whole-second precision accepted).
2. Implement the ONE-SHOT bind under the GLOBAL one-to-one rule (spec
   G7a/G7b): the pane's candidate degree AND the rollout's claimant degree
   are both exactly 1 (a single rollout claimable by two panes binds
   neither — a second provisional claimant would otherwise plan a durable
   merge); detection time truncated to whole seconds before comparison so
   same-second launches qualify; else retry next cycle; explicit
   agent_session always wins; no re-evaluation.
3. Tests (red first): the spec's ambiguity matrix (fresh pane binds; two
   panes/two rollouts stay provisional; one rollout/two panes stay
   provisional; equal-second creation stays provisional; no candidate; and
   explicit identity beats the heuristic).

## Task 5 — G3 + G6: display truth (one commit)

Expected files: `src/tui/view.rs` (paint-time suffix decision; header
summary rename + counts), `src/tui/projection.rs` (label plumbing),
`src/tui/dag.rs` (label call site), `src/tui/app.rs` (the 6 suffix
assertions and the 2 header assertions — enumerate in the report),
`src/herdr/collector.rs` (`summary()` rename — SPLIT from the shared
"controller" literal used by `provider_metadata()`/persisted coverage,
which must not change; count publication from the pane-session derivation),
`src/provider/lane.rs` (label test callers), `docs/tui.md` (row grammar
line and metric-columns section).

1. VERIFY: the paint-time band function (`visible_metric_columns` — TIME
   column present for width ≥ 62), DAG rows bypassing the band, the shared
   name literal between `summary()` and `provider_metadata()`, and that no
   consumer parses the header string (doctor uses the structured vec).
2. G3: suppress the label suffix exactly when the painted band includes
   TIME; keep it below width 62 and in DAG rows. Thread the signal through
   the label constructor; update all call sites.
3. G6: rename ONLY the header summary entry to `ctl=`; add targeted
   pane-session counts (`claude=<n>`; `n/a` and the existing
   `unavailable(detail)` renderings stay) — producer + publication +
   rendering, honestly scoped.
4. Tests (red first): band-boundary paints at widths 61/62; DAG row keeps
   suffix; header assertions for rename + counts + the three provider
   states; persisted/doctor literals unchanged (pin with a test that
   `provider_metadata()` output is byte-stable).

## Verification and process

- TDD per task; red-first proofs by surgical reversion (sha-verified
  restores); load-robustness (10× under 16 spinners) for new
  timing-sensitive tests.
- `mise exec rust@1.97.1 -- make test` and `-- make lint` after each task
  (make test already includes doctests — no separate unpinned invocation).
- Docs commit (spec+plan) first; Task 0 notes as a docs commit; then task
  commits in order.
- Per-task review checkpoints at the implementer wrapper's discretion; one
  final whole-diff review before push with a per-commit map.
- After merge + reinstall: live verification per the spec, WITH BOTH
  PROVIDERS; G7 live checks against a SCRATCH state root; G5 live check is
  time-sensitive (anchor = `earliest_db_event.max(now − window)`) — rely on
  the deterministic fixtures, treat live observation as corroboration.

## Risks

- G2 retires quoted-report free-text evidence with NO replacement linking
  mechanism this increment (user-confirmed product decision, spec G2b):
  nothing that renders correctly today is lost (Task 0 showed neither
  spawn-child family links via the retired scan's real channels), but the
  deferred spawn-child linking increment is now a known display gap for
  wrapper children lacking meta.json/structural evidence. The
  resume-grammar's substring-arming risk is bounded by requiring shell
  invocations and by the existing exact-match-to-discovered-artifact
  admission filter.
- G1a threads timestamps into three fact types — signature changes ripple
  through lane/collector/claude_facts; the declared sets cover them, and
  the epoch-0 guard pins the worst failure mode.
- G1b's closure relies on the persisted `non_lane_task_state_runs`
  ownership set for hook protection — NOT fact recency; a restart test
  pins it.
- G5's mechanism choice is deferred to VERIFY by design; both candidate
  levers are inside the declared set — lever (c), artifact ordering, is
  why `collector.rs` is named there.
- G7 mis-bind is durable; the one-shot unambiguous rule plus the ambiguity
  test matrix plus scratch-root live verification bound the risk.
- G6 rename touches a literal shared with persisted data; the split +
  byte-stability test prevents silent contract drift.
