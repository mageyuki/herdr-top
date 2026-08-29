# Native Binding Merge Persistence and Recovery

## Status

Approved in intent on 2026-08-29 as a corrective delta to PR #21. The user
authorized implementation and addition to PR #21, with an explicit requirement
to exercise the built binary in an adjacent Herdr pane before completion. Code
implementation begins only after this written specification and its derived
implementation plan complete the required review gates.

## Context

The PR #21 binary entered degraded persistence while replaying provider history.
SQLite rejected one retained batch with:

```text
UNIQUE constraint failed: task_runs.native_provider, task_runs.native_session_id
```

The child task under investigation emitted valid start and completion records,
but the failed historical mutation remained pending. Provider ingress then
stayed closed, provider egress saturated, event lag increased, and later child
events did not reach the durable model or normal LIVE projection.

Independent Claude Code and fresh `gpt-5.6-sol` max diagnoses converged on the
retained-batch wedge and the same-batch controller/native merge trigger. Source
inspection identified two independent ways for the batch to claim the same
native binding before `MergeTaskRuns` transfers ownership:

1. `Reducer::finish_provider_observation` derives the final canonical survivor
   and rewrites every earlier upsert for that survivor with the final bound
   projection, including an upsert that originally occurred before the merge.
2. `Store::apply_v6_batch` preseeds a missing survivor from the final
   `batch.task_runs` projection before replaying core operations. The durable
   absorbed native row may still own that binding at preseed time.

The raw failed batch was not retained outside the process. The fix therefore
must be proven with a production-shaped regression rather than by editing the
affected live database or assuming one candidate native session was the exact
collision.

## Goals

1. Preserve the event-time ordering and identity state of core persistence
   operations across provider-observation decoration.
2. Allow V6 foreign-key preseeding without claiming a native binding before a
   same-batch merge transfers or releases its durable owner.
3. Preserve the final authoritative V6 task-run projection after all core
   operations complete.
4. Make exact replay idempotent after the corrected batch commits.
5. Prove that a failed historical mutation can be reconstructed correctly after
   restart, drain durably, reopen provider ingress, and admit later events.
6. Demonstrate the fixed binary against a real Codex child task in an adjacent
   Herdr pane, including cold restart and restore.

## Non-goals

- Do not relax the unique native-session invariant.
- Do not delete, reset, or directly repair the live SQLite database.
- Do not drop, skip, reorder, or synthesize provider events to bypass a failed
  history drain.
- Do not reopen provider ingress while an uncommitted historical mutation is
  still pending.
- Do not change task lifecycle, visibility, retention, token-rate, or terminal
  child semantics except where required to preserve them through the corrected
  persistence transaction.
- Do not add a general migration or recovery command unless database-copy
  testing proves restart/replay insufficient.

## Persistence invariants

The correction must maintain all of these invariants:

1. At every point in a SQLite transaction, at most one canonical task-run row
   owns a `(native_provider, native_session_id)` pair.
2. `PersistV6Batch.operations` retains reducer event-time order. A later merge
   may canonicalize final state but must not retroactively alter an earlier
   operation's binding ownership.
3. Preseeding exists only to satisfy foreign keys for operations that may
   precede their run upsert. It is not an early application of final binding
   ownership.
4. `MergeTaskRuns` remains the operation that transfers a binding from an
   absorbed row to an unbound survivor.
5. After core operations, `batch.task_runs` remains the authoritative final V6
   projection and may update readiness, watermarks, timestamps, and final
   binding state.
6. A transaction failure commits none of the preseed, core, final V6, drain,
   event, agent-node, or association changes.
7. Retrying an already committed corrected batch is a no-op with the same
   durable canonical result.

## Reducer correction

`Reducer::finish_provider_observation` will continue constructing one final
`PersistTaskRunV6` per canonical touched run. That final record is allowed to
use the post-observation model and final canonical binding because the Store
applies it after core operations.

The reducer must stop replacing earlier `PersistOp::UpsertTaskRun` and
`PersistOp::PromoteTaskRunKey` payloads with that final record. Core operations
retain the identity, binding, timestamps, and key that were valid when each
operation was emitted. Existing lineage normalization and canonical touched-run
selection remain unchanged.

This separation gives the batch two explicit layers:

- ordered core operations describe how durable identity changes;
- final V6 task-run records describe the state after those changes.

## Store preseed correction

`Store::apply_v6_batch` will keep its missing-row preseed phase because agent,
event, and edge operations may precede a run upsert and require a durable
foreign-key target.

For a missing run that is a same-batch merge survivor, the preseed must be
binding-neutral. It may use the final V6 record for non-binding state, but its
`native_session` is `None` until ordered core operations transfer ownership or
the final V6 upsert applies the already-valid final projection. Promotion
preseeds retain the existing old-key and binding-neutral behavior.

The implementation must recognize the final survivor through the complete
ordered merge chain rather than only a single direct merge. Non-merge missing
runs retain current preseed behavior.

The Store must not resolve this by disabling the UNIQUE index, temporarily
committing duplicate ownership, deleting the absorbed row, or moving all final
V6 upserts ahead of core operations.

## Historical retry and recovery

An in-process failed historical mutation intentionally retains the exact failed
batch and keeps provider ingress closed. The correction does not mutate that
retained batch in place. Recovery therefore uses a cold restart of the fixed
binary so provider history reconstructs the batch through the corrected reducer
and Store paths.

The preferred recovery sequence is:

1. Stop only the old Herdr Top collector.
2. Preserve the live state root unchanged and make a private copy for testing.
3. Start the fixed binary against the copy and the same read-only provider
   artifacts.
4. Wait for history drains to complete and verify persistence health, provider
   ingress, and later-event admission.
5. Use the unchanged live state root only after the copied-state result proves
   restart/replay sufficient.

If the copied state still wedges, stop. Capture bounded diagnostics and design
an explicit reconciliation change separately; do not improvise live SQL.

## Automated test strategy

### 1. Reducer batch-shape regression

Construct the production identity sequence: an unbound controller survivor, a
native task run owning one Codex SID, and a same-observation merge. Before the
fix, the test must fail because the earlier survivor upsert has been rewritten
with the final binding. After the fix it asserts:

- the pre-merge survivor upsert remains unbound;
- the native row owns the SID until the merge operation;
- operation order is unchanged;
- the final `PersistTaskRunV6` names the canonical survivor and owns the SID;
- absorbed and survivor IDs canonicalize exactly once.

### 2. Real SQLite transaction regression

Seed a temporary Store with the native row already owning the SID, then apply
the reducer-produced V6 batch through `Store::apply_v6_batch`. The test must use
the real Store and schema, not a mock. Before the fix it must reproduce the
native-session UNIQUE failure. After the fix it asserts:

- the transaction commits;
- exactly one canonical row owns the SID;
- the absorbed row points to the survivor and owns no binding;
- event, Agent Node, drain association, and final V6 state reference the
  canonical survivor;
- no partial rows remain from a deliberately failing control transaction.

### 3. Replay and merge-chain boundaries

Apply the corrected batch twice and assert byte-equivalent durable projections.
Add a chained-merge case so binding-neutral preseeding follows the final
survivor across more than one `MergeTaskRuns`. Retain promotion coverage to
prove the new merge handling does not regress the existing old-key preseed.

### 4. Pending mutation and ingress recovery

At the collector/provider integration boundary, force the first historical
submission to return a classified non-durable outcome. Assert that the exact
batch is retained and provider ingress closes. Reconstruct the same observation
through the corrected production path, persist it durably, and assert:

- the pending historical mutation clears;
- provider ingress reopens only after durable acknowledgement;
- a queued later provider event is admitted and persisted;
- persistence recovery is recorded without silently dropping the failed or
  later event.

### 5. Repository gates

Run the targeted reducer, Store, collector, restore, and convergence tests,
followed by `make test`, `make lint`, and `make build`. `cargo fmt --check` and
`git diff --check` must remain clean.

## Adjacent-pane acceptance test

After the integrated build passes automated review, run the newest debug or
release binary in an adjacent pane through the `herdr` CLI. Use a private
temporary `HERDR_PLUGIN_STATE_DIR`; never point an experimental binary at the
live state root first.

1. Start the binary in the adjacent pane and wait for `LIVE` with persistence
   and provider sources available.
2. From this Controller session, start one fresh, bounded Codex child task that
   performs no repository mutation.
3. Read the adjacent pane and verify the child appears beneath the correct
   parent while active and remains as the correct terminal row after completion.
4. Verify no native-session UNIQUE error, retained pending mutation,
   provider-egress saturation, or persistence `DEGRADED` state appears.
5. Run the matching fixed `doctor --json` and record the persistence, skipped
   batch, provider coverage, and freshness fields.
6. Stop and restart the adjacent-pane binary against the same temporary state
   root. Verify the canonical parent/child rows restore without duplication and
   the collector returns to `LIVE`.
7. Repeat the startup/replay check against a private copy of the affected state
   root. Proceed to the real state root only if the copy drains without database
   mutation.

The pane transcript and doctor summary are verification evidence, not committed
fixtures. Temporary state directories remain outside the repository.

## Acceptance criteria

1. The production-shaped regression fails with the observed UNIQUE collision
   before implementation and passes after it.
2. Ordered core operations retain their event-time binding ownership.
3. Merge-survivor V6 preseeds are binding-neutral across direct and chained
   merges.
4. Final V6 state is durable and exactly one canonical row owns each native
   binding.
5. Exact replay is idempotent and promotions retain their existing behavior.
6. Historical failure keeps ingress closed; durable corrected replay clears the
   pending mutation and admits later events.
7. Restart/replay succeeds on a copy of the affected state without SQL repair
   or database reset.
8. The adjacent-pane test shows a real child start, terminal completion,
   healthy doctor output, and duplicate-free cold restore.
9. All repository test, lint, build, formatting, and diff checks pass.

## Rollback

The code change is rollback-safe before live recovery: stop the experimental
binary and return to the previous build without changing the live database.
After the fixed binary first writes to the live state root, rollback must not
reintroduce the defective replay path. Preserve a private state-root backup,
retain the fixed build, and treat rollback as a forward recovery decision rather
than running the old collector against newly drained history.
