# Increment 7: Packaging, Release, and Protocol Compatibility Tiers

**Status:** approved design for implementation planning.
**Baseline:** `main` after PR #7 (interim reviewed protocol set `{19, 20}`,
branch `agent/herdr-protocol-082`, head `d6e3ebc`) is merged. Implementation
must not start from a base that does not contain PR #7, because this increment
replaces the interim protocol contract that PR #7 introduced.
**Design references:** `docs/design/herdr-top-mvp.md` sections 12 (Herdr
plugin packaging), 12.1-12.3, and 20 step 13; PR #7 (interim protocol set).

## Summary

Increment 7 completes the last step of the MVP roadmap (design section 20
step 13): the Herdr plugin manifest with its `[[build]]` artifact fetch,
tag-driven release artifacts for macOS and Linux, and managed-install
validation. It also replaces the interim two-value protocol set from PR #7
with the three-tier protocol compatibility contract (minimum floor Error,
reviewed-set Ok, newer-than-reviewed Warning) together with the tooling that
makes extending the reviewed set an explicit, evidence-backed procedure, and
clears a small mechanical batch carried from Increment 6.

Deferred to Increment 8: `integration install|status|remove` subcommands, the
TOCTOU controller-attestation fix with silent-digest diagnostics, the
SIGHUP-disposition precondition check for the measurement harness, provider
cross-product mapping tests, and the final-review deferred minors from
Increment 6. The `binding_conflicts=2` diagnosis is not an increment task at
all: it is an investigation into live Controller behavior with a suspected
external (herdr-side `agent_session`) component, and proceeds as a standalone
research task.

## Goals

1. A tag-driven GitHub Actions release workflow that builds checksum-verified
   binaries for macOS arm64/x86_64 and Linux arm64/x86_64.
2. `herdr-plugin.toml` and `scripts/fetch-release.sh` exactly as specified in
   design section 12, so `herdr plugin install mageyuki/herdr-top` works
   without a Rust toolchain.
3. Managed-install validation: a real `herdr plugin install` on Linux against
   a published pre-release, and an install smoke on macOS CI runners.
4. Three-tier protocol compatibility in `doctor`, with a schema-review script
   and an in-repo schema baseline that gate additions to the reviewed set.
5. The mechanical batch: gate the featureless-build test, bound hook run
   identifiers, and record the `classify_active_version` rationale.
6. Design-doc updates covering the doctor semantics change and an ADR-candidate
   note for static-linking (musl) should it ever be needed.

## Non-goals

1. Windows support, automatic update, and installing anything into `PATH`
   without explicit user action (unchanged MVP exclusions).
2. Adding the `herdr-plugin` GitHub topic for Marketplace discovery. The
   design defers this until the first release the user declares usable; the
   topic addition is a user action after that declaration.
3. Everything listed above as deferred to Increment 8 or extracted as the
   standalone diagnosis task.
4. Use of any protocol-20-only socket feature. The collector continues to use
   only surfaces present in protocol 19; any future use of a newer-protocol
   feature must be gated on the handshake protocol at the call site.

## Component design

### 1. Release workflow (`.github/workflows/release.yml`)

Triggers: push of tags matching `v*`, and `workflow_dispatch` for a dry run
that builds and checksums everything but publishes no release (artifacts are
retained only as workflow artifacts).

Build matrix:

1. `aarch64-apple-darwin` on `macos-15`.
2. `x86_64-apple-darwin` on `macos-15-intel`. The `macos-15` label resolves to
   an arm64 image and the x64 image carries its own label, so both Apple legs
   build and smoke natively; neither cross-compilation nor Rosetta is
   involved. The `macos-15-intel` label is first exercised by the dry run
   below — if GitHub rejects it, that leg is re-planned rather than silently
   switched to another image.
3. `x86_64-unknown-linux-gnu` on `ubuntu-24.04`.
4. `aarch64-unknown-linux-gnu` on `ubuntu-24.04-arm`.

Each leg runs `cargo build --release --locked` with the stable toolchain,
packages `herdr-top-<version>-<target>.tar.gz` containing the `herdr-top`
binary plus `LICENSE` and `README.md` when present, and runs the local-source
install smoke described in component 2. Linux targets link against glibc
(gnu): the development and CI environments are glibc, and diverging at release
time would create a behavior surface nothing tests. If static linking is ever
required, that is an ADR decision (design section 19 candidate list).

An aggregate job computes a single `SHA256SUMS` file over all four archives
and, for tag builds only, creates a **draft** GitHub Release with the four
archives and `SHA256SUMS` attached. Publishing the release (as a pre-release
first, promoting it later) and pushing tags are user actions; the workflow
never publishes a release on its own.

The `<version>` in artifact names is taken from the tag (`v0.1.0` produces
`0.1.0`) and the workflow fails if it does not equal the `Cargo.toml` package
version, preventing tag/crate version skew.

### 2. Plugin manifest and `scripts/fetch-release.sh`

`herdr-plugin.toml` is added byte-for-byte as specified in design section 12
(id `mageyuki.herdr-top`, version `0.1.0`, `min_herdr_version = "0.8.0"`,
platforms linux/macos, `[[build]]` command `scripts/fetch-release.sh`,
`[[panes]]` running `bin/herdr-top`). The manifest filename and schema are
frozen by the design; if the installed herdr 0.8.2 rejects this manifest, that
is a design-review stop condition, not something to patch around silently.

`scripts/fetch-release.sh` (POSIX-compatible bash, no sudo, no `/dev/tty`, no
`PATH` mutation):

1. Detects the platform with `uname -s`/`uname -m` and maps it to one of the
   four release targets; any other platform fails with a clear message.
2. Reads the pinned release version and the per-target sha256 values from a
   table committed in the repository (`scripts/release-pins.env`, sourced by
   the script). The trust anchor is the repository content at the installed
   checkout, not the release download itself.
3. Downloads
   `https://github.com/mageyuki/herdr-top/releases/download/v<version>/herdr-top-<version>-<target>.tar.gz`
   with curl (bounded retries, fail on HTTP error).
4. Verifies the archive sha256 against the pin (`sha256sum` on Linux,
   `shasum -a 256` on macOS), extracts the binary to `bin/herdr-top`, and
   marks it executable. Checksum mismatch removes the download and fails.
5. Supports a local-source override (`HERDR_TOP_FETCH_LOCAL_DIR`) used only by
   the CI install smoke: it takes the archive from a local directory and
   verifies it against a checksum file in that directory instead of
   downloading. The override path never consults the network, and a checksum
   failure there never deletes the caller's archive (only a bad download is
   removed).
6. Supports a pins-path override (`HERDR_TOP_FETCH_PINS_FILE`) so the fail
   closed "no release pinned" behavior can be tested against a synthetic pins
   file. Without it, the test would pass only until the first real pin commit
   lands and would then start downloading from the network during `cargo
   test`.

Release pin ordering: the workflow computes checksums at build time, so the
pins for a new release land in a follow-up commit after the tag exists
(tag -> build -> user publishes pre-release -> pin-update commit through the
normal PR flow -> managed install validates against the pinned release). The
pins file for a release therefore always describes an already-published
release, and `herdr plugin install`, which reads the default branch, always
sees a consistent pair of pins and artifacts. This ordering is documented in
`docs/guides/release-process.md` (new).

### 3. Managed-install validation

1. Linux (real): after the v0.1.0 pre-release is published and the pin commit
   is merged, run `herdr plugin install mageyuki/herdr-top` against the live
   herdr, confirm the build command fetches and verifies the artifact,
   `bin/herdr-top --version` matches, and the plugin pane launches. Recorded
   as an acceptance step with command output.
2. macOS (CI smoke): every macOS matrix leg of the release workflow runs
   `scripts/fetch-release.sh` in local-source mode against the archive it just
   built and then executes `bin/herdr-top --version`. This validates platform
   mapping, extraction, and the binary on macOS without requiring a published
   release or a macOS machine.
3. Linux matrix legs run the same local-source smoke, so all four targets
   exercise the script.

### 4. Three-tier protocol compatibility

Constants in `src/diagnostics/remote.rs`:

1. `MINIMUM_HERDR_PROTOCOL: u32 = 19` — protocols below this are incompatible.
2. `REVIEWED_HERDR_PROTOCOLS: [u32; 2] = [19, 20]` — protocols whose bundled
   schema has been explicitly reviewed (the review procedure below).

Assessment tiers (total over all protocol values):

1. `protocol < MINIMUM_HERDR_PROTOCOL`, or `protocol` not in the reviewed set
   while `protocol <= max(REVIEWED_HERDR_PROTOCOLS)` (a gap, impossible with
   the current contiguous set but the rule is total): doctor Error, existing
   code `herdr_protocol_mismatch`.
2. `protocol` in `REVIEWED_HERDR_PROTOCOLS`: compatible, `herdr_compatible`.
3. `protocol > max(REVIEWED_HERDR_PROTOCOLS)`: doctor **Warning**, new code
   `herdr_protocol_newer_unreviewed` — monitoring keeps running (every
   inbound wire surface tolerates additive fields once the envelope
   relaxation below lands, and no newer-protocol feature is used), but the
   operator is told the schema delta has not been reviewed.

The runtime collector remains ungated on the protocol value, exactly as today;
all three tiers are doctor diagnostics only.

Doctor observation: the JSON key `supported_protocols` introduced by PR #7 is
kept (stability of the observed shape) and continues to carry the reviewed
set; a `minimum_protocol` key is added. The human renderer needs no change
(generic serialization). The new Warning code joins the closed doctor code
registry and its tests.

Review tooling:

1. `tests/fixtures/herdr-schema/` gains the schema baseline for the highest
   reviewed protocol: the bundled socket-API JSON schema of the herdr release
   that introduced it (herdr 0.8.2, protocol 20), plus a small manifest noting
   version, protocol, and extraction provenance.
2. `scripts/review-herdr-protocol.sh` extracts the bundled schema from a
   candidate herdr binary (the extraction mechanism must be re-verified
   against the installed herdr 0.8.2 during planning — it is an external
   fact), diffs its key-path set against the committed baseline, prints added
   and removed key-paths, and exits nonzero when any key-path is removed or
   when the candidate protocol is already reviewed but its schema differs from
   the baseline. Intended procedure when herdr ships protocol N+1: run the
   script, review the reported delta, then in one change add N+1 to
   `REVIEWED_HERDR_PROTOCOLS`, update the baseline fixture, and record the
   review in the commit.
3. Additive-tolerance pins covering BOTH inbound surfaces, because tier 3's
   "monitoring keeps running" rationale must hold for each of them:
   - Typed payloads (pong, session snapshot and its embedded structures):
     injected unknown fields at every object level must still deserialize.
     A `deny_unknown_fields` regression on any core wire struct fails this.
   - The event-push envelope. `EventStream::next_event` currently rejects any
     frame whose object does not have exactly the two keys `event` and
     `data`, so a protocol that adds a third envelope key (a sequence number,
     a timestamp) would make every pushed event a `MalformedFrame` and stop
     live monitoring while doctor reported only a Warning. The envelope check
     is therefore relaxed to require `event` and `data` and ignore additional
     keys, with tests pinning both the tolerated-extra-key case and the
     still-rejected missing-`data` case.
   `Subscription` needs no pin: it is an outbound request type, not an
   inbound surface.

Design-doc updates: the doctor semantics change (three tiers, the Warning
code), the platform-table protocol note from PR #7 replaced by the three-tier
statement, and the reviewed-set extension procedure referenced from section 12
or the doctor section as appropriate.

### 5. Mechanical batch (one batch task)

1. `tests/workload_harness.rs`: the ungated `#[test]` near line 6999 calls the
   feature-gated `validate_section15_shape_for_test`, so a featureless
   `cargo check --all-targets` fails with E0425 at base. Gate the test behind
   the same `workload-harness` feature. Do not delete it: it is one of the two
   pins backing the recorded reply to a Copilot finding on PR #6.
2. Hook run identifiers accepted by `herdr-top emit --from-hook` are currently
   unbounded; enforce a 128-byte length cap (with the existing charset
   handling) at ingestion, rejecting oversized identifiers with the
   established emit failure policy. 128 bytes covers every observed provider
   identifier form (UUIDs and prefixed hex ids) with an order-of-magnitude
   margin while bounding storage and log growth.
3. `classify_active_version`: add the rationale comment recording that the
   shape-based DateEra predicate (at least two all-digit dot-separated
   components, no floor comparison) is deliberate — herdr's date-era format is
   not a contract herdr-top owns, a semantic predicate risks false
   incompatibility on legitimate future forms, and a wrong-shaped value marked
   compatible only degrades a diagnostic.

### 6. Versioning

`Cargo.toml` stays `0.1.0`; the first tag is `v0.1.0`, published by the user
as a pre-release for validation and promoted by the user when judged usable.
The doctor `standalone_exact` check is unaffected (it compares installed
binary versions, which continue to match).

## Error handling

1. `fetch-release.sh` fails closed on: unknown platform, missing download or
   checksum tool, HTTP failure after bounded retries, checksum mismatch
   (removing the bad download), and absent pins for the detected target.
2. The release workflow publishes nothing when any leg fails; a draft release
   is only created when all four archives and `SHA256SUMS` exist.
3. `review-herdr-protocol.sh` treats schema extraction failure as a hard error
   distinct from "removed key-paths found", with distinct exit codes.
4. The doctor Warning tier changes no runtime behavior; degraded and error
   paths of the collector are untouched by this increment.

## Testing

1. The local gate (fmt, clippy with warnings denied, tests with all targets
   and features, doc tests) stays green throughout, and the featureless
   `cargo check --all-targets` becomes green with batch item 1 and gains a CI
   step so it cannot regress unnoticed. The local gate runs on the installed
   MSRV toolchain while CI lints and tests on stable, so CI remains the
   authoritative gate and a post-MSRV clippy lint may surface only there.
2. Unit matrices for the three tiers: 18 Error, 19 Ok, 20 Ok, 21 Warning, plus
   the below-floor and invalid-version version cases unchanged; doctor golden
   fixtures updated for the observation addition and the new Warning code.
3. The additive-tolerance pin test (component 4).
4. `review-herdr-protocol.sh` self-check: baseline vs itself reports no
   removals and exits zero; a mutated copy with a removed key-path exits
   nonzero (exercised by a repository test or CI step).
5. The release workflow dry run (`workflow_dispatch`) is the integration test
   for component 1, and every matrix leg's local-source install smoke is the
   test for component 2 on all four targets.
6. Managed-install validation on Linux (component 3) is a recorded acceptance
   step, not an automated test.

## Sequencing and integration

1. Repo-code tasks (three-tier doctor change, wire tolerance, schema review,
   mechanical batch) and packaging tasks (manifest, fetch script,
   release-process guide) have disjoint file sets and can proceed as parallel
   implementation tasks under the standard serial-integration rule; docs
   updates ride with their owning task. The release workflow consumes the
   fetch script's local-source contract, so it is not parallel with it: the
   workflow integrates after the fetch script.
2. All tasks integrate serially into one increment branch, published as one
   PR after the final whole-change review, mirroring Increment 6.
3. The release tail happens after that PR merges, because the manifest and
   pins must be on the default branch for `herdr plugin install` to see them.
   Its order is fixed: `workflow_dispatch` dry run first (validating all four
   runner legs while nothing irreversible exists), then the tag push, then
   pre-release publication, then the pin commit, then Linux managed-install
   validation. The pin commit is a small follow-up PR. Tag push and release
   publication are user actions.

## Acceptance criteria

1. Release workflow dry run green on all four targets, producing four archives
   and a `SHA256SUMS` covering them, with every leg's install smoke passing.
2. `v0.1.0` pre-release published (user action) with the four archives and
   `SHA256SUMS` attached.
3. Pin commit merged; on Linux, `herdr plugin install mageyuki/herdr-top`
   completes with the fetch script verifying the pinned checksum, and
   `bin/herdr-top --version` reports 0.1.0.
4. Doctor tiers verified by unit matrix (18 Error, 19 Ok, 20 Ok, 21 Warning)
   and live: against herdr 0.8.2 doctor reports `herdr_compatible`.
5. `review-herdr-protocol.sh` self-check and mutation check behave as
   specified, with the protocol-20 baseline committed.
6. Featureless `cargo check --all-targets` passes; the gated test still runs
   and passes under `--all-features`.
7. Hook run identifier bounding enforced with a regression test; the
   `classify_active_version` rationale comment present.
8. Local gate green per task and CI (stable toolchain, all jobs including the
   new featureless check) green on the increment branch head before
   publication.
