# Backfill fidelity: honest history, honest lineage, honest display

Status: revised after pre-implementation review (round 3)
Branch: `agent/backfill-fidelity`

## Background

After provider ingestion was repaired (the active transcript is tailed again),
live verification with a user watching the TUI surfaced a cluster of fidelity
defects, all observed on a running monitor:

1. **Replayed history displays as live work (G1).** Backfill replays
   historical dispatch/task facts; runs minted from them are stamped with
   receipt time (the collector overwrites the lane-provided receipt clock
   with wall-clock now at `src/herdr/collector.rs` `ProviderEvent::Synthesized`
   handling, and the reducer stamps runs from it), so completed subagent
   tasks from hours earlier appear as `running` rows created seconds ago
   with ticking timers, and nothing closes them.
2. **Quoted UUIDs create false lineage (G2).** The evidence scanner
   intentionally over-emits: any UUID-shaped token in an admitted record
   line yields evidence, so a conversation that merely PASTED other
   sessions' UUIDs (tool output with directory listings and DB dumps)
   admitted two unrelated projects' sessions as lineage children of the
   active pane.
3. **Duplicate time display (G3).** Each run row can show the same
   `end − created_at` duration twice: the label suffix (`· 1m46s`) and the
   metrics-strip TIME column (both derive from `created_at_ms`; the
   summary-overlay `total_duration_ms` is a separate aggregate). One display
   is wanted. Note G3 is only truthful after G1a: before fact-time stamping,
   both copies show the same wrong number.
4. **The inner-codex tier is missing (G4 — investigation, now FIRST).** A
   `codex-implementer` row is the Claude wrapper's agent node; the inner
   `codex exec` writes real rollouts under `~/.codex/sessions` that are
   neither admitted nor linked. The investigation's findings feed G2's
   evidence grammar.
5. **Completed runs show no metrics (G5).** Completed/backfilled runs show
   `—` for model / effort / tokens / tok/s. Root cause (verified in review):
   usage extraction is identical for backfill and live tailing, but the lane
   consumes the usage sample into its dedup set BEFORE attribution, and the
   reducer silently drops telemetry for runs that do not exist yet — so
   ordering (usage seen before the run is minted) loses metrics permanently
   for that pass. Telemetry is transient by design (never persisted); the
   designed recovery is full re-derivation on restart backfill, which the
   same ordering defect then defeats again. The hazard is zero-counting,
   not double-counting.
6. **The sources header line misleads (G6).** `controller=` names herdr-top's
   own control-input socket but reads as "the controlling AI"; provider
   entries are per-provider tri-states without session counts. (The TUI does
   render `n/a` vs `unavailable(detail)` distinctly; the live confusion came
   from width-based truncation of the sources field — the rendering itself
   was not the defect.)
7. **Codex panes never bind (G7).** herdr supplies no `agent_session` for
   codex panes (upstream u4). The monitor mints provisional runs
   (`term_<id>:<ts>:<ordinal>`, `[unlinked]`) that contribute no provider
   targets and can never link; each codex launch adds one.
8. **Provisional runs have no closure path (G8, late addition).** F-Z's
   sweep closure covers Controller-keyed `Queued` runs only, and the 24-hour
   dismissal carve-out is Controller-keyed only — provisional runs
   accumulate forever and cannot even be cleared with `c`.

## Requirements

### G1: fact-time fidelity and closure for replayed history

- G1a. Runs minted or advanced by provider-log facts are stamped with the
  FACT's own timestamp. Mechanics constraints (from review):
  - The overwrite to fix is the collector's `Synthesized` receipt-time
    clobber; the lane already emits fact time in both clocks.
  - Facts that carry no timestamp today (`SubagentAppeared`,
    `SubagentEnded`, `EvidenceId`) must GAIN a real one, with per-source
    precedence: `SubagentAppeared` takes the artifact's `modified_ms` at
    the meta.json read site (no record timestamp exists there);
    `SubagentEnded` and `EvidenceId` take the RECORD timestamp already
    parsed from the JSONL line that produced them (both Claude and Codex
    extractors parse it). Their current `last_append_ms` proxy degenerates
    to "now" on a live parent or to 0 (epoch) when no append was seen — an
    epoch-0 guard test is required. `SubagentEnded` coalescing (multiple
    ended records collapse to one terminal per agent) keeps its existing
    ordinal rule and takes the EARLIEST timestamp among the coalesced
    records (the moment the agent first ended).
  - Agent nodes have only `last_activity_at_ms` (no creation stamp); G1a
    applies fact time to that field, nothing else.
  - Monotonicity: a later fact with an earlier timestamp must not move
    `updated_at_ms` backwards.
- G1b. Closure for replay-minted stale runs: Controller-keyed `Running`
  runs with no LIVE execution — the predicate is explicitly "no
  non-terminal execution attached" (the `apply_lane_close` liveness test),
  NOT the queued arm's `runs_with_executions` set, which counts terminal
  executions and would silently exempt any run that ever had one — AND not
  members of the persisted `non_lane_task_state_runs` ownership set (the
  existing hook-root protection; recency of facts is NOT the guard,
  ownership is), whose fact-time anchor is older than
  `activity::headless_inactivity_ms()`. Close to `ended_unknown` anchored
  at the LAST FACT TIME (not sweep time; deliberately differs from the
  queued arm's `now_ms` stamping; pinned by a test). Implemented as a
  SIBLING arm alongside the queued closure (the differing execution
  predicate and anchor make extension of the queued arm incorrect).
- G1c (was G8). The same sweep also closes `Provisional`-keyed runs with no
  live execution and a stale anchor (`updated_at_ms.or(created_at_ms)`,
  both-None → never close), making them dismissible with `c`.
- G1d. Reopen semantics unchanged.

### G2: lineage requires a typed evidence grammar

- G2a. Evidence positions (closed whitelist), replacing raw line scanning:
  1. Subagent `meta.json` records (existing `SubagentAppeared`).
  2. Wrapper invocations parsed from the transcript's tool-use `command`
     field: recognized spawn/resume command grammars (`codex exec`,
     `codex exec resume <uuid>`, `claude --resume <uuid>`), including
     `CLAUDE_CONFIG_DIR=` prefixes — only inside such commands. (A bare
     `claude -p` spawn cannot carry the child id — the child assigns it —
     so it is NOT part of the grammar; that family is covered by
     position 4.)
  3. Codex structural child references (`sub_agent_activity`
     `agent_thread_id` payloads) in codex_facts.
  4. The tool-use-correlated tool RESULT's typed `session_id` field
     (adding `session_id` to the already-deserialized `ToolUseResult`
     next to its existing `agent_id`): a named field on a typed record,
     categorically distinct from a UUID pasted in a body string. This is
     the documented reliable position — the repository's own measurement
     (`docs/guides/controller-emit-setup.md`) found bare spawn command
     lines carry the id only rarely, while returned results carry it
     reliably — and it is what preserves headless-Claude wrapper lineage
     (`claude -p --output-format json` returns the child `session_id`).
- G2b. Quoted-report evidence (a UUID appearing only in FREE TEXT of a
  message or tool-result body) is RETIRED. Product decision recorded with
  corrected rationale: typed positions 1-4 cover meta.json subagents,
  resume invocations, codex structural refs, and returned session ids;
  what is lost is only lineage that existed NOWHERE except prose. G4's
  findings (which run first) must confirm positions 2-4 against real
  pairs — BOTH an inner-codex wrapper pair AND a headless-Claude
  (`claude -p`) wrapper pair — before this task starts; contradiction →
  stop and re-plan.
- G2b'. Privacy carve-in (deliberate, bounded, POLICY-level): positions 2
  and 4 read fields that the accepted ADR
  `docs/adr/2026-08-24-provider-log-allowlist.md` currently forbids (its
  read list omits `input.command` and any tool-result `session_id`, and
  its never-read section names Bash command bodies and tool-result bodies
  outright). The ADR's read list and never-read section are AMENDED in the
  same commit, recording the bounded carve-in there (only the extracted
  UUID / config-dir / session id is retained; the `sanitize_command_script`
  precedent; non-Debug-exposed transient parse or redacted Debug). The
  existing privacy test `bash_without_description_uses_bare_tool_name`
  (command body never reaches the envelope's Debug output) is PRESERVED,
  not retargeted. `README.md`'s "quoted report" lineage sentence and
  `tests/fixtures/provider-logs/MANIFEST.md`'s read-list mirror are
  updated in the same commit.
- G2c. `scan_raw_ids` remains a pure tokenizer (FIVE production call
  sites parse filenames, not evidence — lane.rs ×4, collector.rs ×1); the
  restriction lands in the extractors (`extract_claude_line` / codex
  adapter), not in the scanner. The `LogFact::EvidenceId` doc comment in
  `facts.rs` describing the old over-emitting behavior is updated in the
  same commit.
- G2d. The canonical design doc (`docs/design/herdr-top-mvp.md`, the
  lineage paragraph) and `docs/guides/controller-emit-setup.md` are amended
  in the same commit; the fixture pinning free-text `CLAUDE_CONFIG_DIR`
  evidence (`config_dir_evidence_is_preserved_from_bash_raw_line`) is
  retargeted to the command-position grammar.
- G2e. No store migration; already-admitted false lineage stops producing
  new facts and ages out.

### G3: one time per row

- The label duration suffix is suppressed exactly when the paint-time
  metric band includes the TIME column (width ≥ 62 in the current bands);
  below that width, and in DAG rows (which render no metric band), the
  suffix remains so time is never lost entirely. This is a paint-time
  decision; the label-construction signature changes accordingly and all
  call sites (dag, projection, lane test callers) are updated. Band
  boundary tests at widths 61/62 and a DAG-row test are required.
  `docs/tui.md` row-grammar and metric-column sections are updated.

### G4: inner-codex tier — investigation only, runs FIRST

- Deliverable `inner-codex-notes.md` from TWO real pairs: (i) an
  inner-codex wrapper transcript + rollout, and (ii) a headless-Claude
  (`claude -p`) wrapper + child transcript. For each: what identity the
  child artifact carries, and whether the parent transcript references it
  in a G2-grammar position (positions 2-4: spawn/resume command,
  config-dir marker, typed tool-result `session_id` — pinning the actual
  wire spelling, `sessionId` vs `session_id`, for position 4) or only in
  prose. A proposed third-tier design sketch is produced for the
  inner-codex pair only. Its findings gate G2. No implementation.

### G5: metrics attribution must survive ordering

- G5a. Usage samples must not be lost to ordering: EITHER unattributable
  usage is retained (bounded, scope-keyed) and re-applied when the run
  appears, OR artifact processing is ordered so run-minting facts precede
  usage. (Deferring dedup-set insertion until attribution succeeds is NOT
  an option: the reducer returns identically on hit and miss, so the lane
  has no success feedback.) The choice between the two is decided at
  implementation VERIFY time; the acceptance is behavioral: backfilling a
  fixture where usage records precede the run-minting facts yields a run
  whose metrics group contains model/effort/tokens (duration is NOT part of
  this criterion — the TIME column derives from `created_at_ms`, not
  usage).
- G5b. Idempotence on re-backfill is preserved (the existing
  `telemetry_survives_backfill_replay_identically` contract): no double
  counting, and — the actual risk — no zero-counting after restart.
- G5c. Out-of-window artifacts unchanged (`—` stays); the out-of-window
  test must use a lineage child, not a pane root (pane roots are
  anchor-exempt).

### G6: sources line tells the truth

- G6a. Rename the control-socket entry in the HEADER summary only
  (`ctl=`, matching the footer); `provider_metadata()` and every persisted/
  doctor-parsed name literal stay unchanged (they share the "controller"
  literal today — the rename must not touch the shared constant without
  splitting it).
- G6b. Provider entries display targeted pane-session counts. This is a new
  producer: the coverage registry holds tri-states only, so a count source
  (from the existing pane-session derivation) must be published to the TUI
  alongside availability. Scope honestly declared as producer + publication
  + rendering.
- G6c. Doctor output and the persisted `source_coverage` event column keep
  their current schema and name literals.

### G7: codex panes bind heuristically until herdr supplies identity

- G7a. For a codex-agent pane with no `agent_session`: bind ONCE, only when
  GLOBALLY unambiguous — a one-to-one match: the pane has exactly one
  candidate rollout AND that rollout has exactly one claimant pane (a
  single rollout visible to two sessionless panes binds NEITHER — the
  identity machinery would otherwise plan a durable merge for the second
  claimant). Candidate window: rollout filename-derived creation time
  (exposed from the existing private parser, whole-second precision) is
  at/after the pane's agent-detection time NORMALIZED to whole seconds
  (truncate the millisecond detection time; a same-second launch must
  qualify). Ambiguity or no candidate → no bind, retry next cycle while
  the pane lives.
- G7b. The bind is FINAL for this increment: no heuristic re-evaluation
  (identity machinery treats a second SID as a hard conflict, and a wrong
  merge is durable). Explicit `agent_session` from herdr, when it exists,
  always takes precedence and suppresses the heuristic.
- G7c. Acceptance: a codex pane launched fresh binds to its rollout within
  one rescan cycle of the rollout appearing — including a same-second
  launch (one pane, one rollout created in the detection second BINDS) —
  yielding a native-keyed run with facts; ambiguity leaves everything
  provisional — pinned by a test matrix: two panes/two rollouts → neither
  binds; one rollout/two panes → neither binds; TWO ROLLOUTS tied in the
  same second for one pane → no bind (candidate degree 2); no candidate →
  no bind; explicit identity wins over the heuristic.
- G7d. Live verification for G7 runs against a SCRATCH state root (a wrong
  merge in the real store would be durable).

## Non-goals

- herdr server fixes (u1 focus ping-pong, u2 silent eviction, u3 tab flap,
  u4 codex agent_session) — separate repository.
- Store migration of existing false-lineage or receipt-stamped rows.
- Widening backfill/admission windows.
- Implementing the inner-codex third tier (G4 is investigation only).
- Doctor/diagnostics schema changes; persisted source-coverage literals.
- Heuristic re-binding / unmerge machinery.

## Acceptance evidence

- `mise exec rust@1.97.1 -- make test` and `-- make lint` green (make test
  already includes doctests; no separate unpinned doctest invocation);
  the two known workload_harness signal flakes remain
  adjudicated-environmental.
- TDD with red-first proofs by surgical reversion (digest-verified
  restores); load-robustness (10× under 16 spinners) for new
  timing-sensitive tests.
- Required test seams from the review are implemented: G1 epoch-0 guard;
  G1 hook-ownership protection across restart; G1 closure anchored at last
  fact time; G2 end-to-end admission test starting from ONLY the pane root
  admitted (existing integration helpers pre-admit children by hand and
  cannot prove production admission); G5 real `.jsonl`+`.meta.json` worker
  fixture asserting exact model/effort/tokens after first pass AND after
  restart, plus a late-metadata cycle; G5 out-of-window via lineage child;
  G7 ambiguity matrix; G3 band-boundary and DAG-row tests.
- Live verification after merge, WITH BOTH PROVIDERS (codex launched as
  part of the check; G7 checks against a scratch state root): no
  resurrection of completed tasks as running rows; no quoted-UUID
  contamination; single time display; recovered metrics on in-window
  completed runs (time-sensitive: the backfill anchor is
  `earliest_db_event.max(now − window)`, so verify against a fresh fixture
  rather than relying on aged live artifacts); sources line shows counts
  and the ctl rename; codex pane binds and shows facts; provisional
  leftovers close and clear with `c`.
