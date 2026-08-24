# Increment 9 live smoke record

Six-point live demonstration of the zero-configuration provider-log lane,
executed against a real Herdr session with live Claude Code and Codex agents
before publication.

**Outcome: 1 pass, 3 partial, 2 fail.** The lane never admitted a provider
artifact in this environment, because the Herdr build in use reports a pane's
agent session as an identifier rather than as an artifact path, and nothing
resolves an identifier to a path. Section "Root cause" records the evidence
chain. The TUI shell, persistence, restart restoration, stall handling, metric
columns, Summary overlay, and the three new `doctor` lane checks were all
exercised successfully and are recorded below.

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-08-25 |
| Host | `srv01`, Linux |
| Herdr | `0.8.2`, wire protocol `20` |
| Claude Code CLI | `2.1.241` |
| Codex CLI | `0.149.0` |
| herdr-top | `0.1.0`, release build from the increment branch at `fad001a` |
| Monitored session | `herdr-top`, one workspace, two panes |
| Pane `w1:p4` | Interactive Claude Code agent, status `working` |
| Pane `w1:p2` | Plain shell, no agent |

Isolation: every run used a throwaway state root
(`XDG_STATE_HOME` pointed at a scratch directory) and an isolated runtime
directory (`XDG_RUNTIME_DIR=/run/user/1000/htsmoke`), so no run touched the
operator's live database or Controller socket. Provider logs were only read.

Capture method: the binary was driven under a pseudo-terminal sized 160x46
with a scripted key schedule; the raw byte stream was replayed through an ANSI
screen emulator to produce the verbatim screens quoted below. A screen-buffer
rendering harness was deliberately not used, so every excerpt is real terminal
output from the real binary.

## 1. Zero-configuration startup — PASS

No herdr-top hook is registered on this machine.

```sh
python3 -c "import json,os;d=json.load(open(os.path.expanduser('~/.claude/settings.json')));\
print(sum(len(m.get('hooks',[])) for a in d.get('hooks',{}).values() for m in a),\
[h['command'] for a in d.get('hooks',{}).values() for m in a for h in m.get('hooks',[]) if 'herdr-top' in h.get('command','')])"
# 5 []
grep -c 'herdr-top' ~/.codex/hooks.json
# 0
```

Five Claude hook commands are registered for unrelated purposes and none
invokes `herdr-top`; the Codex hook file contains no reference. The monitor was
then launched with no flags beyond the isolated environment and rendered its
first frame within eight seconds:

```text
┌ Herdr Top ───────────────────────────────────────────────────────────────────────┐
│host:srv01 | session:herdr-top | up:00:00:07 | workspaces:1 | LIVE | lag:0ms | sources:herdr=available;controller=available;claude=n/a;codex=n/a
└──────────────────────────────────────────────────────────────────────────────────┘
┌ Execution tree ──────────────────────────────────────────────────────────────────┐
│  Session: herdr-top                                                              │
│  └── Workspace: w1                                                               │
│      ├── Tab: w1:t1 (AI)                                                         │
│      │   └── Pane: w1:p4 (UI修正)                                                │
│      │       └── ● Claude 76748191-dc03-486e-a73e-28ec478c0518 · 07s [unlinked]                                                    —     —     —     —    07s
│      │           └── Claude native agent: 76748191-dc03-486e-a73e-28ec478c0518 [state:unknown] [model:unknown] [last:un…
│      └── Tab: w1:t2 (Shell)                                                      │
│>         └── Pane: w1:p2 (mageyuki@srv01: ~/git/mageyuki/herdr-top)              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

Startup itself passes: the binary resolved the session from the environment,
acquired the owner lock, bound its Controller socket, subscribed to Herdr,
rendered the physical topology, and produced a Task Run row for the agent pane
without any configuration. The tree pane inner width was 158, so the widest
metric band applied and all five columns rendered, initially as placeholders.

What did NOT happen is the point of Increment 9: no provider-log content ever
appeared. See sections 2 through 4 and "Root cause".

## 2. Tree with kinds and subjects — PARTIAL

The run kind rendered correctly from the run key as `Claude`. The subject did
not: with no lane fact to supply one, the renderer fell back to the key-derived
identity, so the row shows the native session UUID where a human-readable
subject belongs.

```text
● Claude 76748191-dc03-486e-a73e-28ec478c0518 · 07s [unlinked]
```

The persisted row confirms the absent subject:

```sh
sqlite3 <state>/herdr-top.sqlite3 \
  'select key_kind, key_provider, task_state, subject from task_runs'
# native|claude|running|            <- subject is NULL
```

`[unlinked]` is correct here: no execution or dependency edge links the run,
because no lineage evidence was ingested.

## 3. Live lines — FAIL

No run row ever gained a ` — <live line>` segment across a 150-second and a
100-second observation window, even though the observed Claude agent was
continuously active (`agent_status: working`) throughout both.

## 4. Lifecycle including `ended_unknown` — FAIL for the lane, PASS for stall

No `ended_unknown` transition was observed, because no lane-managed run existed
to close by inactivity. Twenty-four Claude artifacts and seventy-two Codex
artifacts were modified within the twenty-four-hour backfill window, many of
them idle for more than the ten-minute inactivity default, so candidates were
plentiful; none was admitted.

The stall path did fire and is recorded as working. The pane-derived run
received no activity, and at the five-minute default the glyph flipped from `●`
to `⚠` without changing the terminal state:

```text
│      │       └── ⚠ Claude 76748191-dc03-486e-a73e-28ec478c0518 · 05m07s [unlinked]                                                 —     —     —     — 05m07s
```

## 5. Summary overlay with real numbers — PARTIAL

Pressing `s` opened the overlay, which rendered its real scope line and both
tables with correct headers and real counts and durations. Token columns show
the `-` placeholder because no telemetry was ingested.

```text
┌ Summary ─────────────────────────────────────────────────────────────────────┐
│scope: workspace w1 (w: session)                                              │
│                                                                              │
│per worker kind                                                               │
│worker kind | runs | live | total | mean | tok | mean tok/s                    │
│Claude | 1 | 1 | 00s | - | -|-                                                │
│                                                                              │
│per model                                                                     │
│model | runs | live | total | mean | tok | mean tok/s                         │
│unknown | 1 | 1 | 00s | - | - | -                                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

This capture also settles a documentation question: the overlay renders two
separate tables headed `per worker kind` and `per model`, and its rate column
is labelled `mean tok/s`.

## 6. Dismissal and restart persistence — PARTIAL

Restart restoration passes. A second launch against the same state root
restored the durable semantic model rather than starting over: the session
uptime and the run's elapsed time both continued from the first launch's
timeline rather than resetting.

```text
first launch, t=8s      up:00:00:07   ● Claude … · 07s
second launch, t=90s    up:00:05:07   ⚠ Claude … · 05m07s
```

The persisted row confirms a single run whose `created_at_ms` corresponds to
the first launch, and the event ledger accumulated 350 rows across both runs.

Dismissal could not be exercised. `c` dismisses terminal runs and hook-only
runs past their twenty-four-hour boundary; this session had exactly one run,
which was non-terminal and native-keyed, so the key press was a legitimate
no-op and there was no dismissal state to persist across the restart.

## Root cause: the lane admits nothing in this environment

The three new `doctor` lane checks located the failure precisely. Run against
the live owner:

```text
log_lane.readable:  ok      [log_lane_roots_readable]
    observed=[{"provider":"claude","path":"~/.claude/projects","exists":true,"readable":true},
              {"provider":"codex","path":"~/.codex/sessions","exists":true,"readable":true}]
log_lane.freshness: ok      [log_lane_fresh]
    observed={"last_watcher_observation_ms":1787608975403,"age_ms":1001,"stale_after_ms":120000}
log_lane.coverage:  warning [log_lane_coverage_partial]
    observed={"pane_sessions_total":1,"pane_sessions_with_artifacts":0,
              "pane_sessions_without_artifacts":1,"rejected_targets":0}
```

The roots are readable and the watcher is alive and observing, but no pane
session resolved to an artifact, and nothing was rejected. The worker was
running normally: `provider_cycles` reached 27 with `provider_io_errors: 0`,
`invalid_targets: 0`, and `malformed_records: 0`. The database contains no
log-lane event at all:

```sh
sqlite3 <state>/herdr-top.sqlite3 'select source, count(*) from events group by source'
# collector|2
# herdr|348
```

Log-lane events carry `source = "provider-log"`; there are none.

The chain that explains this:

1. Admission is seeded only from the worker's target set. `AdapterProviderWorker::process`
   calls `admit_pane_artifact(target.provider, &target.path)` for each entry of
   `cycle.targets`, and every later stage — discovery filtering through
   `is_admitted_file`, tailing, and fact synthesis — is gated on that admission.
2. Targets are derived only from artifact PATHS. `derive_provider_targets`
   yields a target for a Task Run whose key is `RunKey::NativePath` with a
   non-empty path, or for an Agent Node with a non-empty `session_file`.
3. A `RunKey::NativePath` run is created only when a pane's agent session is
   reported with kind `Path`; a pane reported with kind `Id` produces
   `RunKey::Native` instead.
4. An Agent Node gains a `session_file` only from a provider event, which
   requires a tailed artifact, which requires an admitted target. That is the
   circular dependency.
5. Herdr 0.8.2 reports every agent pane with kind `Id`. Across all three
   running sessions on this machine, every pane with an agent reported
   `"kind":"id"` and no path:

```sh
herdr --session herdr-top pane list
# ..."agent_session":{"agent":"claude","kind":"id","source":"herdr:claude",
#     "value":"76748191-dc03-486e-a73e-28ec478c0518"}...
```

The artifact exists and is current; nothing selects it:

```sh
ls -l ~/.claude/projects/-home-mageyuki-git-mageyuki-herdr-top/76748191-dc03-486e-a73e-28ec478c0518.jsonl
# -rw------- 1 mageyuki mageyuki 19919866 Aug 25 06:56 ...
```

Consequently the zero-configuration path has no entry point on this Herdr
build: the lane is healthy, readable, and watching, and it is watching nothing.

An unrelated `doctor` check disagrees and is worth noting, because an operator
may read it as reassurance. `coverage.native_sessions` classifies a kind-`Id`
pane as fully `covered`, and reported `ok` with `covered: 1` in exactly the runs
where the lane had zero artifacts. The lane check is the honest one.

## Headless child observation

Three headless Codex children were dispatched from a trusted repository
directory during the observation windows, one of which was running for the
entire duration of both TUI launches. Their rollout artifacts were created and
grew normally:

```text
07:03:29  2804484  rollout-2026-08-25T06-56-04-01a035c6-0288-7d22-ab98-cceccc841f0a.jsonl
07:17:09   743842  rollout-2026-08-25T07-14-50-01a035d7-32d4-7852-afda-680fb624ea25.jsonl
07:18:52   586372  rollout-2026-08-25T07-18-36-01a035da-a47a-7150-96cd-66e598d45e95.jsonl
```

None appeared in the tree, in `Unattached`, or in the event ledger. This is the
same admission gap rather than a lineage-evidence gap: with no admitted parent
scope, identifier evidence is never scanned, so the coverage boundary described
in the product documentation was not reachable and could not be demonstrated
either way. No additional cost was incurred to produce these children; they
were the documentation work itself.

## What this record does and does not establish

Established by direct observation:

- Startup, session resolution, owner locking, Controller socket binding, Herdr
  subscription, and topology rendering all work with no configuration.
- The metric column band selection, right-aligned column geometry, and
  placeholder rendering work.
- The stall glyph transition works at its default threshold.
- The Summary overlay's scope line, two-table shape, and headers work.
- Restart restores the durable semantic model with continuous timelines.
- The three new lane health checks report accurately and localized the failure.

Not established, and blocked by the admission gap rather than by test design:

- Lane-derived subjects, run kinds, and live lines.
- Lane lifecycle transitions, including `ended_unknown` by inactivity.
- Token telemetry, `TOK`, `TOK-S`, and the Detail token breakdown.
- Lineage nesting, dispatch-parent placement, and the `Unattached` degradation
  path for headless children.
- Startup backfill of in-window artifacts.
- Dismissal persistence across restart.

These behaviours are covered by the automated test suite. This record states
only that they were not reproducible against a live Herdr 0.8.2 session, and
why.
