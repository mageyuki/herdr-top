# Cold-Safe History Drain Finalization

## Status

Approved in intent on 2026-08-29 as the final corrective increment for PR #21.
The implementation begins only after this specification, its derived
implementation plan, and the required pre-implementation cross-model review
complete their gates.

Spec:
`docs/internal/superpowers/2026-08-29-history-drain-barrier-manifest/spec.md`

## Context

PR #21 now preserves native-session ownership across same-batch task merges and
persists terminal child status for display after Agent Node evidence becomes
stale. A real Codex-controller/Codex-child pane test nevertheless exposed a
separate history-drain failure after cold startup:

```text
invalid persisted history_drains.drain_id value
v1:455628923d6bf7bc7d24fdb949eb2576220fd610a47691c0b1134c0a8f484c4c:
history drain does not exist
```

The collector freezes one immutable `PersistHistoryDrain` manifest for each
provider history scan. Today that manifest reaches SQLite only as decoration on
a novel normalized provider event. The provider worker independently emits a
`HistoryDrainBarrier` containing only the drain ID and observation time. This
creates two uncovered paths:

1. an all-duplicate observation cancels the event mutation before
   `finish_provider_observation` can attach the manifest; and
2. a valid frozen manifest with zero normalized events has no event mutation at
   all.

In both cases the barrier still reaches the writer. Finalization then looks up
the absent `history_drains` row, fails, recovers, and repeats the same failure on
the next retry. Persistence remains degraded and subsequent provider updates
can be skipped.

The pane evidence also showed why shared-state smoke tests are insufficient.
Provider history from unrelated prior sessions can create Unattached Task Runs,
and a controller may not become linkable until its own startup evidence has
been admitted. The final acceptance therefore uses isolated per-cell state and
runtime roots, explicit readiness observation, and one separate shared-history
backfill test.

## Goals

1. Make the history-drain barrier self-sufficient by carrying the exact frozen
   manifest it finalizes.
2. Atomically validate or insert that immutable manifest and finalize its drain
   in one SQLite transaction.
3. Make zero-event, all-duplicate, ordinary, and acknowledgement-loss retry
   drains converge to the same durable result.
4. Preserve fail-closed behavior when the same drain ID is presented with a
   conflicting provider or artifact set.
5. Resolve the remaining PR #21 Copilot minor finding without changing lane
   behavior: characterize the exact working and idle runtime event IDs, then
   remove the avoidable `ExecState` clone.
6. Clear all four non-Grok controller/child cold-test cells with real panes:
   Codex/Codex, Codex/Claude, Claude/Codex, and Claude/Claude.
7. Prove that a production-shaped shared provider-history replay completes
   without a missing manifest, persistence degradation, duplicate native
   binding, or unexplained new Unattached Task Run.

## Non-goals

- Do not add Grok controller or child support. Grok remains a separate pending
  increment.
- Do not change the SQLite schema or run a data migration.
- Do not relax native-session uniqueness, drain immutability, foreign-key
  integrity, or durability acknowledgement rules.
- Do not change Task Run lifecycle semantics, terminal-status precedence, Agent
  Node staleness, task retention, or task-tree ownership rules.
- Do not repair or delete live state. Every experimental cold test uses private
  scratch state; the shared-history test reads provider artifacts but writes
  only to its own scratch database.
- Do not suppress, auto-dismiss, or hide Unattached Task Runs to make the pane
  result appear clean.

## Required invariants

1. A frozen history manifest is immutable. Reusing its drain ID with a
   different provider, artifact identity, digest, byte count, or ordering is a
   hard persistence error.
2. The barrier's drain ID equals its manifest's drain ID before it can enter the
   pending queue or writer.
3. Manifest upsert and drain finalization commit together or neither commits.
4. A finalization retry after a known or unknown acknowledgement returns the
   same finalized page and does not duplicate artifacts, associations, events,
   or native bindings.
5. Event mutations remain responsible for event and model persistence, not for
   guaranteeing that the drain row exists.
6. A history drain containing no novel provider events is still durably
   represented and finalized.
7. Provider ingress reopens only after durable finalization acknowledgement.
8. Existing strict read and query APIs continue to reject a nonexistent drain;
   only the barrier-owned finalization transaction may create its missing row.
9. Transaction failure leaves the previous durable state unchanged.

## Design

### Barrier-owned manifest

`HistoryDrainBarrier` will own an `Arc<PersistHistoryDrain>` in addition to the
observation time and acknowledgement state. Construction validates that the
manifest identity is internally consistent and makes the manifest's drain ID
the single source of truth; callers cannot independently supply a mismatching
ID.

The collector obtains this value from the already-frozen provider manifest when
it enqueues the barrier. Pending-queue merge, hold, retry, and acknowledgement
paths retain the same immutable `Arc`. `StagedHistoryFinalization` carries that
manifest through the reducer boundary to the persistence writer. No early
return in event normalization is responsible for preserving it.

### Atomic finalization transaction

The Store will factor its existing manifest immutability checks so the writer's
finalization operation can execute the following sequence inside one
transaction:

1. validate and idempotently upsert the barrier-owned manifest;
2. reject any conflicting row for the same drain ID;
3. finalize the drain and derive the finalized page using the transaction's
   state; and
4. commit the manifest and finalization together.

The public writer command carries the complete manifest rather than only a
drain ID. The ordinary manifest-upsert path remains available for event-bearing
observations; it becomes an idempotent optimization rather than a correctness
precondition. Existing finalization readback and acknowledgement semantics are
preserved.

### Retry and failure behavior

If the first finalization commit succeeds but acknowledgement is lost, the held
barrier retries with byte-identical manifest input. The immutable upsert and
finalization are idempotent and return the same durable page. If the commit is
known not to have occurred, the same barrier remains held until a later durable
outcome. A manifest conflict or invalid identity is never retried as an
alternate manifest and never downgraded to a warning.

### Copilot cleanup

`root_runtime_state_event` will match the runtime state by reference to select
the event-ID suffix, then move the owned state into the emitted event. A new
characterization test must first prove both emitted state and exact event-ID
suffix for `Working` and `Idle`. The change must not add `Copy` to `ExecState`
or alter any provider-lane decision.

## Automated test strategy

### 1. Store transaction tests

Add a real SQLite test that invokes barrier finalization without a prior event
or manifest upsert. Before the fix it fails with `history drain does not exist`;
after the fix it asserts that the manifest and finalized page commit together.

Add boundary cases for:

- exact retry after successful commit;
- simulated acknowledgement loss followed by retry;
- a conflicting manifest for an existing drain ID;
- a deliberately failing finalization that leaves no partial manifest row; and
- the existing ordinary event-bearing drain path.

### 2. Collector and reducer tests

Exercise the production barrier path with:

- a frozen manifest containing zero normalized events;
- an observation whose normalized events are all duplicates;
- an ordinary observation with at least one novel event; and
- a held barrier retried after each classified writer outcome.

Each case asserts that the exact frozen manifest reaches
`StagedHistoryFinalization`, ingress remains closed until durable
acknowledgement, and no event is synthesized merely to persist the manifest.

### 3. Queue and identity tests

Update barrier constructor and pending-queue coverage to prove that ordinary
slots cannot overtake a barrier, queue merge retains one byte-identical
manifest, and a mismatching identity is rejected before persistence.

### 4. Provider-lane characterization

Add the Working and Idle event-state/event-ID assertions before removing the
clone in `src/provider/lane.rs`.

### 5. Repository gates

Run targeted Store, reducer, collector, provider, restore, status, and
convergence tests. Then run `cargo fmt --check`, `make test`, `make lint`,
`make build`, and `git diff --check`.

## Four-cell cold-pane acceptance

The four required cells are:

| Controller | Child | Required result |
| --- | --- | --- |
| Codex | Codex | linked working to done, then linked done after restart |
| Codex | Claude | linked working to done, then linked done after restart |
| Claude | Codex | linked working to done, then linked done after restart |
| Claude | Claude | linked working to done, then linked done after restart |

Each cell uses a fresh private state database and cell-specific runtime and
evidence roots. It starts the newest integrated binary in a real Herdr pane,
waits for persistence and provider sources to report ready, establishes the
controller row, and only then starts one bounded child carrying a unique marker.
The child performs no repository mutation and does not delegate further.

For every cell, capture and verify:

1. the pane transition from working to done under the exact controller;
2. the exact parent edge, provider/native session ID, Task Run, and Agent Node
   in SQLite;
3. terminal display after Agent Node evidence crosses the production staleness
   boundary, with the automated clock-boundary test as the deterministic
   assertion and the pane/database evidence as the real-process assertion;
4. a cold stop and restart against the same private state, followed by the same
   linked done row without duplication;
5. `PRAGMA quick_check`, foreign-key integrity, and uniqueness of every non-null
   native binding;
6. healthy persistence with zero relevant not-committed, durability-unknown,
   skipped-update, binding-conflict, and provider-disagreement counters; and
7. no unexplained Unattached Task Run first observed during that cell's time
   interval.

For Claude controller or child cells, retain the raw hook payloads in a bounded
temporary evidence directory. A stop-only extra Task Run or delayed controller
link is a failed cell until those payloads and provider artifacts identify its
source; elapsed startup time and the point at which the controller becomes
linkable are recorded for every cell.

## Shared-history cold acceptance

After the four isolated cells pass, run one additional scratch instance against
the real provider history roots to reproduce the production backfill shape.
This run is not a fifth controller/child matrix cell and must not write to the
live state database.

The run must:

1. complete every frozen history drain, including providers with no novel
   events;
2. return to and remain in healthy live collection without the missing-manifest
   error or periodic re-degradation;
3. drain the persistence queue with no skipped updates or duplicate native
   binding;
4. cold restart from the scratch database and converge to the same associations
   and terminal statuses;
5. pass doctor, SQLite quick check, and foreign-key checks; and
6. account for every newly created Unattached Task Run from provider evidence,
   treating any unexplained row as a blocker rather than a cosmetic issue.

## Expected implementation tasks and files

The implementation plan must preserve these two serial integration units:

1. History-drain correctness: `src/provider/mod.rs`,
   `src/herdr/collector.rs`, `src/reducer.rs`, `src/store/writer.rs`, and
   `src/store/mod.rs`.
2. Provider-lane characterization and clone cleanup: `src/provider/lane.rs`.

The two implementation tasks have disjoint declared file sets and may be
implemented concurrently in separate linked worktrees after the plan review.
The Controller integrates and verifies them one at a time. Any implementation
touching a file outside its declared set stops integration until scope and
disjointness are reassessed.

## Acceptance criteria

1. The missing-manifest regression is red before implementation and green after
   implementation through the real Store and writer path.
2. Barrier finalization atomically creates or validates the exact frozen
   manifest and finalizes it without depending on a novel provider event.
3. Zero-event, all-duplicate, ordinary, and acknowledgement-loss retry cases
   converge idempotently.
4. Conflicting manifests fail closed with no partial durable change.
5. Existing event ordering, task lifecycle, terminal display, retention, and
   native-binding uniqueness tests remain unchanged and passing.
6. The lane cleanup is protected by exact Working and Idle characterization
   tests and introduces no public or behavioral change.
7. All four real-pane controller/child cold cells pass the complete per-cell
   evidence contract.
8. The shared provider-history scratch run and its cold restart pass without a
   missing manifest, degraded persistence, duplicate binding, skipped update,
   or unexplained new Unattached Task Run.
9. Repository verification, mandatory final cross-model review, CI, and latest
   HEAD Copilot review complete with no unresolved actionable finding before
   PR #21 is declared ready. The PR is not merged by this work.

## Rollback

Before any private scratch run, rollback is the normal code rollback because no
schema or live data changes occur. A failed scratch run is stopped and its
evidence retained; it is never repaired in place to manufacture a pass. Because
the change is additive to barrier payload and transaction sequencing, reverting
the code restores the previous binary format without a database migration. The
known missing-manifest build must not be restarted against live state as a
recovery method.
