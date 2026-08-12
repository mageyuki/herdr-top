# Increment 5: Reliability Hardening and Measured Performance

- Status: approved design, awaiting implementation-plan approval
- Approved: 2026-08-12
- Subject revision: `9cd98131038a53b6dd36ff53e9b89825acba70ae`
  (merged PR #1)
- Research workspace:
  `~/.research/mageyuki--herdr-top/increment-5-reliability-performance/`

## Summary

Increment 5 establishes a reproducible performance baseline before optimization,
closes the D1 and D2 reliability debts structurally, and replaces the static TUI
event-lag value with measured runtime health. It does not assume that D4 needs an
incremental algorithm. D4 is authorized only when end-to-end measurements miss a
section 15 target and isolated measurement shows that D4 consumes at least 25%
of reducer-plus-publish work at the failing workload.

The work is intentionally layered:

1. add a deterministic workload harness and record the unchanged production
   algorithm at the subject revision;
2. harden persistence ownership and writer-panic acknowledgement handling;
3. publish measured lag, load, and performance-induced `DEGRADED` state; and
4. reach an explicit measurement checkpoint before considering D4.

Absolute wall-clock, CPU, and RSS values are authoritative only on the selected
Linux reference profile. Shared CI gates deterministic behavior, losslessness,
state transitions, and result-schema validity, not host-dependent timing.

## Goals

- Exercise the section 15 target and overload workloads end to end with synthetic,
  deterministic data.
- Record an untouched baseline before D1, D2, performance-health, or optimization
  changes can affect the measurements.
- Make the event-ledger mirror have exactly one mutation authority.
- Guarantee that an issued persistence permit cannot wait forever if the writer
  thread panics before acknowledging it.
- Define `event_lag` from a monotonic runtime clock and show it in the existing
  header surface.
- Enter `DEGRADED` truthfully when measured lag or load exceeds the supported
  envelope, without dropping Task Runs or edges.
- Preserve enough machine-readable evidence to compare the baseline and each
  subsequent stage.
- Use measurements, rather than graph size alone, to decide whether D4 warrants
  another implementation stage.

## Non-goals

- No speculative D4 incrementalization.
- No database migration or persistence of performance telemetry.
- No arbitrary write-acknowledgement timeout that could reclassify a merely slow
  durable write as failed.
- No dropping, sampling, or truncating Task Runs, execution edges, or dependency
  edges to meet performance targets. Rendering may continue to omit older
  activity as already allowed by the MVP contract.
- No hardware-counter requirement and no privileged kernel, cgroup, or systemd
  configuration change.
- No claim that the selected workstation is physically a 4-core, 16-GB host.
- No production optimization outside a separately evidenced and approved scope.
- No push, PR, merge, release, history rewrite, reset, squash, or rebase as part
  of design or planning.

## Binding contract and current gaps

The binding performance contract is section 15 of
`docs/design/herdr-top-mvp.md`. Its target is 50 live panes, 200 live or
default-visible Task Runs, 1,000 dependency edges, 20 events/s sustained, a
100 events/s burst for ten seconds without loss, screen update p95 below one
second, input response below 100 ms, startup below three seconds with 100,000
retained events, idle CPU below 2%, and memory below 100 MB on the reference
machine. Above target, the product must report lag and become `DEGRADED` without
losing Task Runs or edges. Acceptance criterion 29 additionally requires twice
the sustained target for 60 seconds to degrade visibly without structural loss.

The subject revision has four relevant gaps:

| Gap | Evidence at `9cd9813` | Design response |
| --- | --- | --- |
| D1 | `WriterClient` is `Clone`, and the shared `EventLedgerCache` is mutated by both the client/permit side and writer thread. Production currently has one normal command owner, so the defect is latent and topology-dependent. | Remove cloneable command authority and make one collector-owned authority serialize every mirror transition. |
| D2 | `AcknowledgementObservationGuard::drop` suppresses failure publication while panicking, while `PendingEnqueue::wait` waits on a oneshot without a competing writer-health observation. | Publish typed unhealthy state from the writer unwind boundary and race pending acknowledgement against that state. |
| Event lag | `main.rs` always supplies `Duration::ZERO` to `HeaderInputs.event_lag`. | Track oldest admitted-but-incomplete reducer work with a monotonic clock and publish a runtime snapshot. |
| D4 | `dangling_announcement_components` scans graph state on graph-affecting updates, and publishing clones the complete `DomainModel`; correctness is already restored. | Measure both scopes. Change D4 only if the two-part authorization gate passes. |

The contract names 1,000 **dependency** edges. Runtime telemetry also records
execution-edge count, but does not invent an execution-edge threshold. In
particular, the supported target must not become degraded merely because its
1,000 dependency edges coexist with ordinary execution edges.

## Considered approaches

### A. Layered custom harness first — selected

Build a purpose-specific deterministic workload harness, record the current
algorithm, then add reliability and health behavior in stages. This is the only
approach that directly covers startup, ingress-to-screen latency, input latency,
losslessness, idle resources, and overload behavior while preserving a clean
pre-change baseline.

### B. Criterion-first component benchmarks

Statistical microbenchmarks would compare reducer, graph, cloning, and rendering
components precisely, but would not by themselves prove the end-to-end section
15 contract. They also bias the work toward D4 before an end-to-end bottleneck is
known. Focused component timing may be added inside the custom harness when the
D4 decision point is reached; Criterion is not the primary contract.

### C. Production telemetry first

Adding lag and degradation before measuring would expose UI behavior sooner, but
would couple the baseline to new instrumentation and make instrumentation cost
harder to distinguish from existing behavior. The clean baseline is more
valuable, so telemetry follows the harness.

## Stage 1: deterministic workload harness and baseline

### Workload model

The harness uses synthetic identifiers and content and exposes reusable builders
for exact topology and event sequences. All correctness runs use an injectable
monotonic clock. Reference runs use the real monotonic clock under the selected
host controls.

The core workload profiles are:

| Profile | Topology and event stream | Required observation |
| --- | --- | --- |
| Target topology | 50 live panes, 200 live/default-visible Task Runs, 1,000 dependency edges | Exact final pane, run, dependency-edge, and execution-edge counts; no loss |
| Sustained target | Target topology plus 20 admitted reducer events/s for 60 seconds | No loss and no rate-induced degradation at the exact boundary |
| Target burst | Target topology plus 100 admitted reducer events/s for ten seconds | No loss; one- and ten-second window boundaries remain within the envelope |
| Startup | A store with exactly 100,000 retained event-ledger rows and target topology | First usable rendered frame below three seconds on the reference profile |
| Idle | Restored target topology with no new events after settling | CPU below 2% and maximum RSS below 100 MB on the reference profile |
| Twice-target overload | Target topology plus 40 admitted reducer events/s for 60 seconds | Visible `DEGRADED` by 60 seconds, reported lag/load reason, and no structural loss |

An "admitted reducer event" enters the ordered reducer queue. Frames rejected by
wire validation before queue admission do not enter the performance rate or lag
metric. Every admitted event, including a semantic no-op or typed reducer
rejection, reaches one terminal reducer outcome and therefore leaves the pending
set.

The event schedule uses sequence numbers and half-open clock intervals so exact
boundaries are deterministic. A rate window at time `now` contains admission
timestamps in `(now - width, now]`. The exact target values are allowed; only
strictly greater values breach an envelope.

### Measured outcomes

The harness records the following rather than inferring them from a single total
runtime:

- end-to-end screen-update latency, from admitted tagged event to the first
  `TestBackend` frame containing its resulting model marker; p95 must be below
  one second;
- reducer lag, from admission until the reducer produces the event's terminal
  outcome, plus publish-to-render time as a separate bottleneck diagnostic;
- input response, from injected key event until the first rendered frame that
  reflects the state transition; p95 must be below 100 ms;
- startup, from opening the prepared state to the first usable rendered frame;
- process-tree user plus system CPU time divided by elapsed idle time, expressed
  as one-core percentage and required to remain below 2%;
- maximum resident set size for the measured process tree, required to remain
  below 100 MB;
- submitted, admitted, completed, persisted, and rendered sequence coverage;
- exact final counts and stable identities for panes, Task Runs, dependency
  edges, and execution edges; and
- scoped reducer, D4 analysis, model-clone/publish, and render timings used for
  bottleneck attribution, never as substitutes for the end-to-end result.

Losslessness is an exact invariant, not a percentage. All admitted sequence
numbers must have terminal outcomes, every expected durable event must be in the
ledger, and final structural identities and counts must match the generated
oracle. A command failure, missing sequence, duplicate unexpected outcome, or
malformed result invalidates the trial.

### Samples and comparison

Each reference scenario performs at least one unrecorded warm-up followed by
five recorded trials. Startup performs ten recorded fresh-process trials because
it yields only one latency sample per process. Latency profiles retain all event
samples and report each trial's count, minimum, median, p95, p99, and maximum.
Every recorded trial must satisfy losslessness; every per-trial p95 and absolute
resource threshold must pass. Results may not select the best trial or silently
discard an outlier.

The first baseline is identified by both the harness revision and the production
subject revision `9cd9813`. Adding the harness may change test-only or harness
code, but it must not change the measured production behavior before the baseline
is recorded. Later stage reports use the identical workload schema and include a
machine-comparable baseline identifier.

### Result document

The runner emits one versioned JSON document per run and preserves its raw tool
output alongside it. The JSON contains at least:

- schema version, scenario, pass/fail/invalid status, and closed failure reasons;
- production subject SHA, harness SHA, tracked-clean verdict, build profile, and
  Rust/rustc identity;
- exact executed command and controlled environment keys;
- kernel, architecture, CPU model and topology, selected CPU IDs, memory total,
  storage kind, governor/boost state when readable, and ambient load;
- applied CPU-affinity and address-space limits, plus an explicit
  `true_cgroup_memory_limit: false` field for the selected profile;
- warm-up and recorded-trial counts;
- raw observations and derived distributions with units and thresholds;
- all submitted/admitted/completed/persisted/rendered and structural counts; and
- D4/reducer/publish scoped timings when present.

Schema tests reject unknown enum values, missing required fields, mixed units,
non-finite values, incomplete trials, and inconsistent aggregate counts. The
runner writes the result only after validation and exits nonzero for an invalid
or failed authoritative run.

Raw reference results are stored in the research workspace, outside every Git
worktree. They are not committed. Synthetic workloads must not include prompts,
tokens, credentials, provider transcript content, or private session values.

## Selected reference profile

The current workstation is the authoritative constrained Increment 5 profile:

- Linux x86_64 on an AMD Ryzen 7 5700X;
- CPU IDs 0, 1, 2, and 3, verified as four distinct physical cores;
- `taskset -c 0-3` applied to the complete measured process tree;
- `prlimit --as=17179869184` applied as an inherited 16-GiB virtual-address
  ceiling;
- GNU `/usr/bin/time -v` and `pidstat` for elapsed time, CPU, and maximum RSS;
  and
- local NVMe storage.

Before an authoritative run, the runner fails closed if the required commands
are absent, CPU topology no longer maps the selected IDs to four distinct
physical cores, the requested affinity or address-space limit was not applied,
the build or scenario command fails, or tracked Git content is dirty. The
tracked-clean check examines index and tracked worktree differences only; it
does not enumerate, read, reject, stage, ignore, or otherwise act on unrelated
untracked user files.

This profile is not a true 16-GB physical host and has no writable 16-GiB memory
cgroup. The user systemd bus is unavailable and the current cgroup v2 scope is
read-only. Every report must state that limitation and must not claim cgroup
isolation. The 100-MB maximum-RSS requirement remains directly enforceable and
is much stricter than the address-space ceiling.

Hardware performance counters are unavailable under `perf_event_paranoid=4`.
The implementation must not require `perf`, `sudo`, or a policy change.

## Stage 2: D1 persistence authority

Operational writer command capability becomes uniquely owned by the collector's
persistence facade. The command owner is non-`Clone`; APIs that reserve capacity,
query or reserve event IDs, enqueue batches, apply acknowledgement deltas, and
initiate ordinary maintenance require access through that owner. The lifecycle
handle is limited to shutdown/join and cannot issue ordinary persistence work.

Persistence health remains a clonable, read-only watch receiver. TUI,
diagnostics, tests, and pending acknowledgements may clone that receiver, but a
health receiver conveys neither command authority nor ledger mutation access.

The in-memory ledger mirror is loaded before the writer thread starts and is no
longer shared with that thread. Its transitions are serialized as follows:

1. the unique authority checks the mirror for duplicate IDs;
2. accepted IDs are reserved in memory before the already-owned channel permit
   is consumed, preserving within-process deduplication;
3. the writer owns SQLite and returns a typed acknowledgement containing the
   exact durable result and exact cleanup rows deleted by that transaction; and
4. the unique authority applies only those exact cleanup deltas to matching
   `(event_id, seen_at)` mirror entries.

Known-not-committed and durability-unknown results do not blindly remove an
accepted in-process reservation. Keeping it is the conservative process-lifetime
deduplication behavior and avoids replaying an event whose durable disposition is
unknown. Seven-day cleanup removes only an exact entry confirmed deleted by the
writer.

The implementation is incomplete if a production path can clone an operational
sender, acquire a second mutation path to the ledger mirror, or mutate the mirror
inside the writer thread.

## Stage 2: D2 panic and acknowledgement closure

Writer execution maintains the active `PersistenceOperation` at a thread unwind
boundary. If an operation panics before sending its typed acknowledgement, that
boundary publishes the existing closed `PersistenceStatus::Degraded` shape with
the operation-specific acknowledgement failure and an unknown durability
disposition. The thread then terminates; it does not attempt to continue using a
possibly inconsistent SQLite owner.

Every pending acknowledgement receives a persistence-health subscription created
before its command is sent. Waiting selects between:

- the typed acknowledgement, which wins when available because it carries the
  most precise durability result; and
- a transition to degraded writer health, which terminates the wait with the
  published typed failure when no acknowledgement is available.

The waiter checks the current watch value before sleeping and after wake-up, so a
health transition between send and poll cannot be lost. Channel closure without
a prior typed transition publishes and returns the operation-specific unknown
acknowledgement failure. No elapsed-time timeout participates in the production
decision.

Deterministic tests inject a panic after a permit has been issued and the command
has become writer-owned but before acknowledgement. A test timeout may guard the
test process, but is not product behavior. The regression asserts that:

- the pending wait terminates;
- persistence health becomes typed `Degraded` for the active operation;
- durability is not claimed committed or not committed when it is unknown;
- later command reservation is denied; and
- shutdown/join observes the terminated writer rather than hanging.

The existing healthy-after-panic expectation is replaced; preserving it would
leave D2 open.

## Stage 3: runtime performance snapshot

A runtime-only `PerformanceSnapshot` is published over a read-only watch channel.
It is initialized empty on every process start and is neither serialized to the
database nor added to migrations. It contains:

- monotonic `event_lag`;
- pending admitted-event count and admission/completion sequence high-water
  marks;
- admitted event counts in rolling one-, ten-, and 60-second windows;
- live pane, live/default-visible Task Run, dependency-edge, and execution-edge
  counts; and
- a stable ordered set of closed performance degradation reasons.

### Event lag

At reducer admission, the tracker records `(sequence, Instant)`. At every terminal
reducer outcome, it removes that sequence. `event_lag` is `now` minus the oldest
remaining admission instant and is zero only when the pending set is empty. It
never uses provider timestamps, wall-clock time, or event IDs as ordering inputs.

When lag first becomes greater than one second, the tracker latches the current
admission high-water sequence as the breach generation. The lag reason remains
active until every sequence at or below that high-water mark has completed. If
lag is still above one second then, a new generation is latched; otherwise the
reason clears. This prevents a partially drained backlog from flickering between
`LIVE` and `DEGRADED`.

The TUI header reads the published snapshot instead of a static
`Duration::ZERO`. Tests use a virtual monotonic clock and exact sequence
transitions; production uses `Instant`-based time.

### Supported load envelope

The following exact values are within the envelope; strictly greater values
activate their corresponding reason:

| Dimension | Maximum within envelope |
| --- | ---: |
| Live panes | 50 |
| Live/default-visible Task Runs | 200 |
| Dependency edges | 1,000 |
| Admitted events in `(now - 1s, now]` | 100 |
| Admitted events in `(now - 10s, now]` | 1,000 |
| Admitted events in `(now - 60s, now]` | 1,200 |
| Oldest pending admitted event | 1 second |

The 60-second value models the 20 events/s sustained target. The ten-second value
admits the isolated 100 events/s burst. At 40 events/s, the 60-second count
crosses 1,200 after 30 seconds, ensuring that the twice-target profile is visibly
degraded before the 60-second acceptance deadline even if the reducer keeps lag
below one second.

Rate reasons clear naturally only after enough admissions age out of their
rolling window. Multiple simultaneous reasons are retained and rendered in a
stable order, so clearing one reason cannot hide another.

### Quality composition

Performance does not replace physical/source observation truth. The existing
quality precedence remains, from strongest to weakest:

1. `DISCONNECTED` when physical observation is unavailable;
2. `RECONCILING` while restoring or reconciling;
3. `DEGRADED` when physical observation is otherwise live but an existing source
   degradation or any performance reason is active; and
4. `LIVE` only when none of the above applies.

The snapshot exposes closed reasons such as pane count, visible-run count,
dependency-edge count, each rate window, and event lag. It never embeds raw event
data or unbounded diagnostic strings. Existing wire and persisted schemas do not
change.

## Stage 4: measurement-gated D4 decision

D4 incrementalization requires both conditions:

1. the unchanged/full-recompute implementation misses at least one section 15
   target in the authoritative end-to-end profile; and
2. at the failing workload, isolated D4 analysis time is at least 25% of measured
   reducer-plus-model-publish time, using the same warm-up and recorded trials.

The numerator is time spent computing dangling announcement components. The
denominator covers the reducer operation through publication of the cloned model
for the same measured events. The JSON records both raw scoped samples and the
derived ratio.

If either condition is false, D4 remains unchanged. Expected lag only above the
supported target is not a target miss when degradation is truthful and the run
is lossless. If both conditions are true, work stops at the checkpoint and a
focused D4 design and implementation-plan amendment is proposed before code is
changed. A non-D4 target bottleneck likewise requires a measurement-backed design
amendment rather than an opportunistic optimization.

## Error handling and invalid evidence

The harness and reference runner fail closed for:

- topology/control mismatch or a dirty tracked subject;
- workload generation, build, process, database, render, or tool failure;
- incomplete or malformed JSON;
- missing or duplicate sequence outcomes;
- any Task Run or edge loss;
- an unmet authoritative threshold; or
- a baseline whose production subject cannot be proven to be the requested SHA.

`failed` means a valid measurement violated the contract. `invalid` means the
measurement could not establish the contract. Neither is reported as a pass.
Ambient load and unavailable optional metadata are recorded; they do not silently
excuse a failure. Required-control metadata is never optional.

## Verification strategy

Shared CI and normal local verification cover:

- deterministic workload generation and exact oracle counts;
- virtual-clock one-, ten-, and 60-second boundary and recovery cases;
- lag onset, generation-latched recovery, simultaneous reasons, and quality
  precedence;
- exact target remaining non-degraded and twice target degrading by 60 seconds;
- target, burst, and overload no-loss invariants;
- D1 compile-time/API ownership constraints plus ledger-delta behavior;
- D2 panic-after-permit, health transition, prompt waiter termination, and denied
  future reservation;
- versioned JSON schema validation and fail-closed runner behavior;
- cross-platform build of deterministic components, with Linux-only reference
  execution selected at runtime; and
- the repository's standard formatting, lint, unit, and integration suites.

Shared CI does not fail on absolute wall-clock, CPU, or RSS observations from an
arbitrary runner. The selected Linux profile is the authoritative gate for those
values. Reference results are reproduced after the baseline, D1/D2, and runtime
performance-health stages so regressions can be attributed to a stage.

## Completion criteria

Increment 5 reaches its measurement checkpoint when all of the following are
true:

1. a valid baseline for production subject `9cd9813` exists in the research
   workspace;
2. D1 has one non-cloneable operational authority and no writer-thread ledger
   mirror mutation;
3. D2's injected writer panic degrades health and terminates an issued pending
   acknowledgement without a production timeout;
4. the header reports measured monotonic event lag;
5. exact target boundaries remain non-degraded, and overload/lag breaches publish
   stable closed reasons with correct recovery;
6. the target, burst, and twice-target workloads preserve all Task Runs, execution
   edges, dependency edges, and admitted event outcomes;
7. final authoritative reference results honestly pass or fail every section 15
   threshold without host-independent CI substituting for them; and
8. the D4 ratio and authorization decision are recorded, with no D4 code change
   unless both gate conditions pass and the follow-up design is approved.

A valid measured failure is useful evidence but is not mislabeled completion of
the missed performance target. It becomes the input to a scoped follow-up design.

## Implementation boundary and process constraints

The later implementation plan must split work into task-specific branches and
project-local linked worktrees and must declare the expected file set for every
task before deciding whether tasks are independent enough to run concurrently.
Integration remains serial and Controller-owned.

Before implementation, the completed plan receives exactly one mandatory
read-only review using actual `claude-opus-5`, with a separate fresh
`gpt-5.6-sol` maximum-effort supplement. Fable dispatch and health checks remain
disabled. These reviews validate the plan against the actual repository,
installed command behavior, this design, and section 15; they do not replace
user approval.

No implementation plan is part of this document. It is created only after the
user reviews this committed design artifact. No branch is pushed and no PR or
release action is taken without a later explicit request.
