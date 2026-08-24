# Increment 9 live smoke record

Six-point live demonstration of the zero-configuration provider-log lane,
executed against a real Herdr session with live Claude Code and Codex agents
before publication. This is the post-fix re-run at `78e68c4`; the earlier run
at `fad001a` is retained below as before/after evidence.

**Outcome: 6 pass, 0 partial, 0 fail. Previous outcome: 1 pass, 3 partial,
2 fail.** The identifier-kind admission defect from the previous run is
resolved. Provider-log events, human-readable subjects, live lines,
`ended_unknown`, real token metrics, dismissal, and restart restoration were
all directly observed. A restart-only rejected-target counter anomaly remains
and is documented separately; it did not prevent the six demonstrations.

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-08-25 |
| Host | `srv01`, Linux |
| Herdr | `0.8.2`, wire protocol `20` |
| Claude Code CLI | `2.1.241` |
| Codex CLI | `0.149.0` |
| Rust toolchain | Cargo `1.97.1` |
| herdr-top | `0.1.0`, release build at `78e68c4` |
| Monitored session | `herdr-top`, one workspace, two panes |
| Pane `w1:p4` | Interactive Claude Code agent, status `working`, session kind `id` |
| Pane `w1:p2` | Plain shell, no agent |

The release binary was built from the tested worktree before capture:

```sh
cargo --version
# cargo 1.97.1 (c980f4866 2026-06-30)
cargo build --release --locked
# Finished `release` profile [optimized] target(s) in 12.36s
git rev-parse --short HEAD
# 78e68c4
```

Isolation was mandatory for every owner launch. `XDG_STATE_HOME` named one
fresh private scratch state base and `XDG_RUNTIME_DIR` named one fresh private
runtime directory; both were outside the repository and outside the operator's
normal Herdr Top roots. The retained capture timeline records the exact values
and shows that the owner's persistent writable descriptors were its scratch
lock, log, SQLite database, WAL/SHM files, and runtime directory only.

The operator database and runtime sentinel were statted before and after the
entire protocol. Their metadata was identical:

```text
operator database: inode=1596926 size=6897664 mtime=1787491086 ctime=1787491086 mode=600
operator sentinel: inode=93 size=9       mtime=1787231189 ctime=1787231189 mode=600
```

The isolated Controller socket existed only while each scratch owner was live
and was removed on orderly exit. The live Herdr server and agent panes were
queried read-only and were never signalled or modified. Provider logs were
opened read-only; no settings, hooks, transcripts, or rollouts were changed.

Capture method: the release binary was driven under a pseudo-terminal sized
160x46 with scripted keys. Each raw byte stream was replayed through an ANSI
terminal emulator to recover the verbatim screens below; no screen-buffer
rendering harness was used. The two required observation windows lasted
151.021 seconds and 101.058 seconds. A 12.052-second no-input restart verified
dismissal read-back, and a final 25-second owner supplied the human-readable
`doctor` excerpt.

The documented inactivity override was necessary to demonstrate lifecycle
closure within a practical window:

```text
HERDR_TOP_HEADLESS_INACTIVITY_MS=30000
```

The system `sqlite3` executable was unavailable, so the exact required SQL was
executed read-only through Python's standard `sqlite3` module. This was the only
protocol tooling substitution. No additional Claude or Codex invocation was
started; the already-running Codex session performing this verification was
used for the headless-child observation, so the re-run incurred no additional
agent cost.

## 1. Zero-configuration startup — PASS

No Herdr Top hook is registered on this machine. The first number below is the
count of all Claude hook commands; the second is the subset invoking
`herdr-top`.

```sh
python3 -c "import json,os; d=json.load(open(os.path.expanduser('~/.claude/settings.json'))); print(sum(len(m.get('hooks',[])) for a in d.get('hooks',{}).values() for m in a), sum(1 for a in d.get('hooks',{}).values() for m in a for h in m.get('hooks',[]) if 'herdr-top' in h.get('command','')))"
# 5 0
python3 -c "import os; p=os.path.expanduser('~/.codex/hooks.json'); print(open(p).read().count('herdr-top') if os.path.exists(p) else 0)"
# 0
```

The release binary was launched with no command-line flags. At seven seconds it
had resolved `HERDR_SESSION=herdr-top`, acquired the scratch owner lock, bound
the scratch Controller socket, subscribed to Herdr, and rendered the live
topology. The following are verbatim line prefixes from that frame; only the
right-hand blank area and borders are cropped:

```text
│host:srv01 | session:herdr-top | up:00:00:07 | workspaces:1 | DEGRADED | lag:0ms | perf:events_10s+events_60s | sources:herdr=available;controller=available;…
│  Session: herdr-top
│  ├── Workspace: w1
│  │   ├── Tab: w1:t1 (AI)
│  │   │   └── Pane: w1:p4 (UI修正)
│  │   │       └── ● Claude 76748191-dc03-486e-a73e-28ec478c0518 · 07s [unlinked]                                                    —     —     —     —    07s
│  │   └── Tab: w1:t2 (Shell)
│  │       └── Pane: w1:p2
```

`DEGRADED` here came from the intentionally heavy startup backfill crossing the
performance event-rate thresholds, not from a startup, lock, database, Herdr,
or Controller failure. The first frame and later lane data both arrived without
hook registration or provider configuration.

## 2. Tree with kinds and subjects — PASS

The initial identifier fallback was replaced during backfill by the provider
kind and subject. By 49 seconds the pane-bound row and its children carried
human-readable subjects and worker kinds. These are verbatim row prefixes:

```text
│  │   │       └── ● claude-code UI修正 · 49s                                                                                  fable-5 xhigh 29.1k 0.1/s    49s
│  │   │           ├── ● codex-implementer I9 Task 6 lifecycle state · 49s                                                           —     —     —     —    49s
│  │   │           ├── ● codex-reviewer Review I9 Task 12 diff · 42s                                                            opus-5   max   729 0.0/s    42s
```

The required Task Run query was executed exactly. To avoid republishing real
task subjects, its result was aggregated by subject presence after retrieval:

```sql
select key_kind, key_provider, task_state, subject from task_runs;
```

```text
controller||completed|NULL|rows=361
controller||completed|present|rows=20
controller||ended_unknown|NULL|rows=1
controller||ended_unknown|present|rows=16
controller||failed|NULL|rows=1
controller||running|present|rows=2
native|claude|running|NULL|rows=1
```

The quoted screen is the monitor's own privacy-filtered projection. No raw
prompt, response, transcript, tool argument, or tool output was inspected or
copied into this record.

## 3. Live lines — PASS

Live-line segments appeared during the 150-second window. The following two
row prefixes are verbatim from the 145-second frame; `— system` and
`— assistant` are the live-line segments between subject and duration:

```text
│  │   │           ├── ● claude-code herdr-top increment 6 emit hardening 実装 — system · 02m14s                               fable-5 xhigh  224k 0.5/s 02m14s
│  │   │           ├── ● claude-code v0.8.2対応とIncrement7 — assistant · 02m14s                                                opus-5 xhigh  257k 0.7/s 02m14s
```

This directly reverses the previous run's point 3 failure, where neither the
150-second nor the 100-second window produced a live line.

## 4. Lifecycle including `ended_unknown` — PASS

With `HERDR_TOP_HEADLESS_INACTIVITY_MS=30000`, lane-managed rows crossed the
inactivity boundary and rendered the terminal `◌` glyph. The 145-second frame
contained these verbatim row prefixes, among others:

```text
│  │   │           ├── ◌ codex-reviewer Review I9 Task 12b diff · 06s                                                                —     —     —     —    06s
│  │   │           ├── ◌ codex-implementer I9 Task 10 columns shedding filter · 04s                                                  —     —     —     —    04s
│  │   │           ├── ◌ codex-reviewer Review I9 Task 11 diff · 04s                                                                 —     —     —     —    04s
```

The database immediately after the first 151-second launch independently
confirmed the lane state:

```sql
select task_state, count(*) from task_runs group by task_state order by task_state;
```

```text
completed|136
ended_unknown|27
queued|2
running|9
```

The override demonstrates the transition mechanics, not the unmodified
ten-minute timing policy. No live process was stopped to manufacture a
terminal state.

## 5. Summary overlay with real numbers — PASS

Pressing `s` opened the same two-table overlay as the earlier run, now with real
output-token totals and rates. These are verbatim text rows from the overlay in
the 95-second screen; borders and trailing spaces are cropped:

```text
scope: workspace w1 (w: session)
per worker kind
worker kind | runs | live | total | mean | tok | mean tok/s
Claude | 3 | 3 | 00s | - | 153k | 0.1/s
claude-code | 20 | 20 | 00s | - |124k | 0.3/s
per model
model | runs | live | total | mean | tok | mean tok/s
claude-fable-5 | 3 | 3 | 00s | - | 153k | 0.1/s
claude-opus-5 | 10 | 10 | 00s | - | 124k | 0.3/s
unknown | 10 | 10 | 00s | - | - | -
```

The earlier point 5 partial result showed only `-` placeholders for token
columns. This run directly establishes the real-number path.

## 6. Dismissal and restart persistence — PASS

Restart restoration remained continuous. The first launch reached
`up:00:02:24`; the 100-second restart began at `up:00:04:45`, and the no-input
read-back restart showed `up:00:06:42`. These are the durable session timeline,
not three independent seven-second sessions.

At 12 seconds in the second launch, `c` stamped 180 eligible terminal rows at
one receipt time:

```sql
select count(*) from task_runs where dismissed_at_ms is not null;
select task_state, count(*) from task_runs
where dismissed_at_ms is not null group by task_state order by task_state;
```

```text
180
completed|153
ended_unknown|27
```

The third launch sent no `c`. Its read-back retained 177 dismissal markers,
all with the exact timestamp written by the second launch:

```sql
select count(*), min(dismissed_at_ms), max(dismissed_at_ms)
from task_runs where dismissed_at_ms is not null;
```

```text
177|1787612948547|1787612948547
```

Three dismissed rows had become eligible for retention cleanup between the two
queries; the surviving rows retained the original marker rather than being
recreated as visible undismissed runs. This demonstrates both the dismissal
write and restart persistence.

## Lane and database evidence

The final live-owner `doctor` read-back reported all four relevant checks as
`ok`:

```sh
HERDR_SESSION=herdr-top XDG_STATE_HOME=<state> XDG_RUNTIME_DIR=<runtime> \
  HERDR_TOP_HEADLESS_INACTIVITY_MS=30000 target/release/herdr-top doctor
```

```text
log_lane.readable: ok [log_lane_roots_readable] log-lane roots are readable observed=[{"provider":"claude","path":"~/.claude/projects","exists":true,"readable":true},{"provider":"codex","path":"~/.codex/sessions","exists":true,"readable":true}]
log_lane.coverage: ok [log_lane_coverage_complete] pane-session artifact coverage is complete observed={"pane_sessions_total":1,"pane_sessions_with_artifacts":1,"pane_sessions_without_artifacts":0,"rejected_targets":115900}
log_lane.freshness: ok [log_lane_fresh] log-lane watcher is fresh observed={"last_watcher_observation_ms":1787613153396,"age_ms":739,"stale_after_ms":120000}
coverage.native_sessions: ok [coverage_complete] native-session coverage is complete observed={"covered":1,"partial":0,"uncovered":0,"total":1,"unknown_provider":0,"by_provider":[{"provider":"claude","covered":1,"partial":0,"uncovered":0,"total":1},{"provider":"codex","covered":0,"partial":0,"uncovered":0,"total":0}]}
```

The final isolated ledger contained hundreds of provider-lane events rather
than the previous zero:

```sql
select source, count(*) from events group by source;
```

```text
collector|4
herdr|696
provider|213
provider-log|669
```

`provider-log` is the synthesized fact source; `provider` contains lane
liveness and close observations. The final database also contained 74 Claude
Agent Nodes.

## Previous failure and resolution

The earlier Task 13 run at `fad001a` recorded the following failure evidence:

```text
Outcome: 1 pass, 3 partial, 2 fail
log_lane.coverage: warning [log_lane_coverage_partial]
observed={"pane_sessions_total":1,"pane_sessions_with_artifacts":0,
          "pane_sessions_without_artifacts":1,"rejected_targets":0}
events by source: collector|2, herdr|348, provider-log absent
```

Herdr 0.8.2 supplied the pane as `AgentSessionKind::Id`. The old target derivation
provided only artifact paths, while an Agent Node could acquire a `session_file`
only after an artifact was already tailed. That circular dependency admitted
nothing.

Commit `78e68c4` breaks the cycle in four linked steps:

1. `derive_provider_targets` carries provider-attributed identities from
   pane-bound `RunKey::Native` runs.
2. `TargetSet` carries those session identities alongside artifact paths.
3. `AdapterProviderWorker::process` seeds `Admission::admit_pane_session` and
   enumerates that provider's standard and derived roots even with no path
   target.
4. `coverage.native_sessions` treats an identifier-kind pane as uncovered while
   runtime diagnostics still report zero artifacts.

The new `provider-log|669`, `pane_sessions_with_artifacts:1`, live lines,
tokens, and lifecycle closures independently demonstrate that the previous
failure is resolved rather than hidden by the doctor presentation.

## Residual rejected-target counter anomaly

The successful restart run exposed a separate diagnostic defect. Restored
Claude subagent nodes have valid operational paths ending in
`subagents/agent-*.jsonl`. `derive_provider_targets` in
`src/herdr/collector.rs` promotes every non-empty Agent Node `session_file` to a
pane artifact target. `AdapterProviderWorker::process` then calls
`Admission::admit_pane_artifact`, whose Claude pane-root parser intentionally
accepts only `<uuid>.jsonl`. Each `agent-*.jsonl` target is therefore counted as
invalid on every provider cycle. The observed restart contained 20 such paths,
and the final doctor sample accumulated `rejected_targets:115900` despite
complete `1/1` pane coverage.

The exact chain is `derive_provider_targets` at
`src/herdr/collector.rs:4620`, the target validation loop at
`src/herdr/collector.rs:3842`, `Admission::admit_pane_artifact` at
`src/provider/lane.rs:1306`, and the UUID-only Claude parser at
`src/provider/lane.rs:1428`. The six-point functionality continued to work, so
this is not softened into a smoke failure. No follow-up code fix was made: the
dispatch authorized a bounded fix only if the lane still admitted nothing,
which was disproved by the live event ledger.

## Headless child observation

The already-running verification session supplied a zero-extra-cost headless
Codex rollout with session ID `01a03600-3239-78f0-a541-7fc376613370`. Its file
existed and grew during the smoke; only metadata was inspected:

```text
2527825 bytes  rollout-2026-08-25T07-59-37-01a03600-3239-78f0-a541-7fc376613370.jsonl
```

It did not appear in Task Runs, Agent Nodes, or events:

```text
task_runs|0
agent_nodes|0
events|0
```

The monitored Herdr session had a Claude pane and no Codex pane. The live
session admissions therefore contained only provider `Claude`, and
`AdapterProviderWorker::process` enumerated standard roots only for providers
present in that set (`src/herdr/collector.rs:3859`). No explicit Codex artifact
path or completed parent evidence admitted this still-running rollout. The
active-child case is consequently not established by this smoke. No extra
child was launched, because doing so outside the monitored pane would not add
lineage evidence and would add cost without changing the coverage boundary.

## What this record does and does not establish

Established by direct observation:

- Zero-configuration startup, session resolution, scratch ownership,
  Controller binding, Herdr subscription, and physical topology rendering.
- Identifier-kind pane admission, provider artifact discovery, synthesis, and
  complete `doctor` coverage.
- Provider-derived worker kinds, human-readable subjects, model/effort fields,
  output-token totals, rates, and live lines.
- Lane lifecycle closure through `ended_unknown` with the disclosed 30-second
  override.
- Summary's two-table shape and real token values.
- Dismissal writes and restart persistence of their receipt-time marker.
- The previous zero-admission failure is resolved by `78e68c4`.

Not established or intentionally bounded:

- The default ten-minute `ended_unknown` delay; the transition was observed
  with the documented 30-second override.
- The selected-Task-Run Detail token breakdown; the scripted Detail key landed
  on a tab row, so only Summary and row telemetry are established.
- A live inner Codex child, `Unattached` Codex degradation, or cross-provider
  lineage from a Claude pane. The zero-cost rollout had no admitted Codex scope.
- Complete steady-state backfill performance on this large live artifact. The
  screens remained `DEGRADED` under event-rate and visible-run thresholds while
  useful data continued to arrive.
- Correct rejected-target accounting after restart. The counter anomaly above
  is directly observed and remains unfixed.

No raw transcript, prompt, response, tool argument, or tool output was used as
quoted evidence. The record relies on live Herdr inventory, provider-file
metadata, the monitor's privacy-filtered screens, `doctor`, and the isolated
database only.
