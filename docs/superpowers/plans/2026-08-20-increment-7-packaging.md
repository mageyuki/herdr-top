# Increment 7: Packaging, Release, and Protocol Compatibility Tiers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the MVP's final roadmap step — plugin packaging with tag-driven,
checksum-verified releases — plus the three-tier herdr protocol compatibility
contract and a small mechanical batch carried from Increment 6.

**Architecture:** Packaging is three artifacts (a GitHub Actions release
workflow, the `herdr-plugin.toml` manifest, and `scripts/fetch-release.sh`
with repo-pinned checksums) whose trust anchor is repository content, not the
download. Protocol compatibility becomes a total three-tier function in
`src/diagnostics/remote.rs` consumed only by doctor, with a committed schema
baseline and a review script gating future reviewed-set extensions; the
inbound wire surfaces (typed payloads and the event-push envelope) are pinned
additive-tolerant so the Warning tier's monitoring-keeps-working rationale
holds everywhere.

**Tech Stack:** Rust (stable in CI, MSRV 1.97.1 locally), bash, jq, GitHub
Actions (`macos-15`, `macos-15-intel`, `ubuntu-24.04`, `ubuntu-24.04-arm`
runners), GitHub Releases via `gh` CLI.

**Spec:** `docs/superpowers/specs/2026-08-20-increment-7-packaging-design.md`

## Global Constraints

1. Base: `main` at or after `be58348` (the PR #7 merge:
   `SUPPORTED_HERDR_PROTOCOLS = [19, 20]`, observation key
   `supported_protocols`). Implementation MUST NOT start from a base without
   it; stop and report if absent.
2. MSRV-local gate, run per task: `cargo fmt --all -- --check`,
   `cargo clippy --locked --all-targets --all-features -- -D warnings`,
   `cargo test --locked --all-targets --all-features`,
   `cargo test --locked --doc`. This runs on the locally installed 1.97.1
   toolchain; CI's stable-toolchain lint/test jobs are AUTHORITATIVE, and a
   clippy lint introduced after 1.97.1 may still fire only in CI — treat that
   as a normal CI-fix round, not a local-gate failure.
3. Worktrees lack the untracked `mise.toml`, so run cargo with
   `PATH="$HOME/.cargo/bin:$PATH"` (resolves rustc/cargo 1.97.1).
4. The doctor JSON key `supported_protocols` is kept verbatim (observed-shape
   stability); the reviewed-set constant is renamed, the JSON key is not.
5. No `deny_unknown_fields` may be added to any wire struct.
6. `cargo test` accepts ONE positional test-name filter; never pass two
   (the second is rejected as an unexpected argument).
7. Both `src/diagnostics/remote.rs` and `src/hook_adapter.rs` test modules
   import symbols with explicit `use super::{...}` lists (no glob); every new
   symbol a test uses must be added to that list.
8. Commit messages use the repository's conventional style
   (`feat(...)`/`fix(...)`/`test(...)`/`docs(...)`/`ci(...)`) and end with the
   standard `Co-Authored-By` trailer.
9. Two known-environment pitfalls: two `workload_harness` SIGHUP-trap tests
   fail in shells with non-default SIGHUP disposition (not a regression; CI is
   authoritative); macOS injects `__CF_USER_TEXT_ENCODING` into children.

## Task dependency and parallelism map

1. Task 1 is self-contained (remote.rs + doctor.rs + tests/doctor.rs in ONE
   task; the split variant was rejected in plan review because the enum
   change forces doctor edits in the same change).
2. Tasks 2, 3, 4, 5 are mutually independent and independent of Task 1
   (disjoint file sets); dispatch in parallel under the standard
   serial-integration rule.
3. Task 6 consumes Task 5's local-source contract: integrate AFTER Task 5
   (output dependency — do not dispatch in parallel with it).
4. Task 7 (design-doc text) integrates last.
5. Phase R (release tail) happens only after the increment PR merges, and
   its dry run precedes the tag.

---

### Task 1: Three-tier protocol assessment and doctor Warning tier

**Files:**
- Modify: `src/diagnostics/remote.rs` (consts at lines 15-17,
  `assess_herdr_compatibility` at lines 59-83, test-module imports at lines
  674-682, protocol matrix test at lines 975-1023,
  `classify_active_version` doc line 629 / fn line 630)
- Modify: `src/doctor.rs` (code registry ending near line 501, observation
  struct at lines 229-235, `herdr_compatibility_check` at lines 506-537 —
  note it contains THREE places that must change: the `normalized` match,
  the observation constructor, and the assessment match — and
  `i4_doctor_compatibility_matrix` at lines 2162-2200)
- Modify: `tests/doctor.rs` (mirrored code-message registry near lines
  60-79, golden JSON constants at lines 26/28/30, fixture builders
  `healthy_report()` observation literal ~lines 271-279, `warning_report()`
  ~line 316, `error_report()` ~lines 440-449)

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub const MINIMUM_HERDR_PROTOCOL: u32`,
  `pub const REVIEWED_HERDR_PROTOCOLS: [u32; 2]`, enum variant
  `HerdrCompatibility::NewerUnreviewed { version: String }`, doctor code
  `"herdr_protocol_newer_unreviewed"` (Warning, message "Herdr protocol is
  newer than reviewed"), observation field `minimum_protocol: u32`
  (serialized between `found_protocol` and `supported_protocols`).
  `SUPPORTED_HERDR_PROTOCOLS` no longer exists.

- [ ] **Step 1: Write the failing remote.rs tests.** Add
  `MINIMUM_HERDR_PROTOCOL` and `REVIEWED_HERDR_PROTOCOLS` to the test
  module's `use super::{...}` list (lines 674-682). Replace the protocol part
  of `i4_remote_herdr_version_protocol_matrix` and add the invariant test:

```rust
        // Reviewed protocols are Compatible (19 and 20, at two versions each).
        for protocol in [19_u32, 20] {
            for version in ["0.8.0", "1.25.300"] {
                assert_eq!(
                    assess_herdr_compatibility(&pong(version, protocol)),
                    HerdrCompatibility::Compatible {
                        version: version.to_owned(),
                    }
                );
            }
        }
        // The version floor precedes every protocol tier: a below-floor
        // release reports VersionTooOld whichever tier its protocol lands in,
        // so hoisting a protocol check above the floor cannot go unnoticed.
        for protocol in [0_u32, 18, 19, 20, 21, u32::MAX] {
            assert_eq!(
                assess_herdr_compatibility(&pong("0.7.9", protocol)).issue(),
                Some(HerdrCompatibilityIssue::VersionTooOld)
            );
        }
        // Below the floor protocol: hard mismatch.
        for protocol in [0_u32, 18] {
            assert_eq!(
                assess_herdr_compatibility(&pong("99.0.0", protocol)).issue(),
                Some(HerdrCompatibilityIssue::ProtocolMismatch)
            );
        }
        // Newer than every reviewed protocol: tolerated but unreviewed.
        for protocol in [21_u32, u32::MAX] {
            assert_eq!(
                assess_herdr_compatibility(&pong("99.0.0", protocol)),
                HerdrCompatibility::NewerUnreviewed {
                    version: "99.0.0".to_owned(),
                }
            );
        }
```

```rust
    #[test]
    fn i7_reviewed_protocol_set_is_ascending_and_floored() {
        assert!(REVIEWED_HERDR_PROTOCOLS.windows(2).all(|w| w[0] < w[1]));
        assert!(REVIEWED_HERDR_PROTOCOLS
            .iter()
            .all(|p| *p >= MINIMUM_HERDR_PROTOCOL));
    }
```

- [ ] **Step 2: Write the failing doctor tests.** In
  `i4_doctor_compatibility_matrix`: extend the `current` (0.8.0/19)
  assertions, replace the protocol-21 expectation (it currently expects
  `herdr_protocol_mismatch`), and add a below-floor case:

```rust
        assert_eq!(current_observed.minimum_protocol, 19_u32);
        assert_eq!(current_observed.supported_protocols, vec![19, 20]);

        let newer = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.9.0".to_owned(),
            protocol: 21,
            capabilities: None,
        }));
        assert_eq!(newer.status, CheckStatus::Warning);
        assert_eq!(newer.code, "herdr_protocol_newer_unreviewed");
        assert_eq!(newer.observed.unwrap().found_protocol, 21_u32);

        let below = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.9.0".to_owned(),
            protocol: 18,
            capabilities: None,
        }));
        assert_eq!(below.status, CheckStatus::Error);
        assert_eq!(below.code, "herdr_protocol_mismatch");
```

  In `tests/doctor.rs`:
  1. Mirrored registry (lines 60-79): add
     `"herdr_protocol_newer_unreviewed" => "Herdr protocol is newer than reviewed",`
     (it panics on unknown codes, so the golden below needs this first).
  2. All three golden constants: insert `"minimum_protocol":19,` immediately
     after `"found_protocol":<n>,` in the `compatibility.herdr` observed
     object.
  3. ERROR golden (line 30): change that observed `"found_protocol":21` to
     `"found_protocol":18`, and mirror `found_protocol: 21` ->
     `found_protocol: 18` in the `error_report()` struct literal
     (~line 446). This is a coherence change, not a compile necessity: a
     fixture asserting `herdr_protocol_mismatch` for 21 would describe a
     state the code can no longer produce.
  4. WARNING golden (line 28) now exercises the new code: in
     `warning_report()`, override the herdr pong to version `"0.9.0"`,
     protocol `21` and override `compatibility.herdr` to the Warning check —
     mirror the construction pattern `error_report()` uses for its
     `compatibility.herdr` override (~lines 440-449), with status Warning,
     code `herdr_protocol_newer_unreviewed`, message
     `Herdr protocol is newer than reviewed`, observed
     `found_version "0.9.0" / minimum_version "0.8.0" / found_protocol 21 /
     minimum_protocol 19 / supported_protocols vec![19, 20]`. Update the
     WARNING golden accordingly: `herdr.pong` observed becomes
     `{"version":"0.9.0","protocol_version":21}` and `compatibility.herdr`
     becomes
     `{"status":"warning","code":"herdr_protocol_newer_unreviewed","message":"Herdr protocol is newer than reviewed","observed":{"found_version":"0.9.0","minimum_version":"0.8.0","found_protocol":21,"minimum_protocol":19,"supported_protocols":[19,20]}}`.
     Keep the pong and compatibility entries coherent (same version and
     protocol); everything else in the fixture stays unchanged.

- [ ] **Step 3: Run to verify failure** (one filter per invocation —
  Global Constraint 6):

Run: `cargo test --locked --lib i4_remote_herdr_version_protocol_matrix`
Expected: compile error (`MINIMUM_HERDR_PROTOCOL`, `REVIEWED_HERDR_PROTOCOLS`,
`NewerUnreviewed` unresolved) — the compile failure is the red state.
Run: `cargo test --locked --test doctor`
Expected: same compile-stage red (crate-wide), for the same reason.

- [ ] **Step 4: Implement `src/diagnostics/remote.rs`.** Replace lines 15-17
  (the two doc-comment lines plus the const; line 18 is the doc comment of
  `MINIMUM_HERDR_VERSION` and must remain):

```rust
/// Oldest Herdr socket protocol the typed probes accept at all.
pub const MINIMUM_HERDR_PROTOCOL: u32 = 19;
/// Herdr socket protocols whose bundled schema has been explicitly reviewed
/// (ascending; 0.8.0 ships 19, 0.8.2 ships 20 with an additive-only change).
/// Extend ONLY through the procedure in `scripts/review-herdr-protocol.sh`.
pub const REVIEWED_HERDR_PROTOCOLS: [u32; 2] = [19, 20];
```

Add the enum variant (the enum currently has `Compatible` and `Unavailable`):

```rust
    /// Handshake is usable, but the protocol is newer than every reviewed one.
    NewerUnreviewed { version: String },
```

and make `issue()` return `None` for it. Replace the protocol check inside
`assess_herdr_compatibility` (keep the version-floor check above it intact):

```rust
    if pong.protocol < MINIMUM_HERDR_PROTOCOL {
        return HerdrCompatibility::Unavailable {
            reason: HerdrCompatibilityIssue::ProtocolMismatch,
            version: Some(version.normalized),
        };
    }
    if REVIEWED_HERDR_PROTOCOLS.contains(&pong.protocol) {
        return HerdrCompatibility::Compatible {
            version: version.normalized,
        };
    }
    let newest_reviewed = *REVIEWED_HERDR_PROTOCOLS
        .last()
        .expect("reviewed protocol set is non-empty");
    if pong.protocol > newest_reviewed {
        return HerdrCompatibility::NewerUnreviewed {
            version: version.normalized,
        };
    }
    // A gap inside the reviewed range (impossible while the set is
    // contiguous, but the tier function stays total).
    HerdrCompatibility::Unavailable {
        reason: HerdrCompatibilityIssue::ProtocolMismatch,
        version: Some(version.normalized),
    }
```

Update the function doc (line 59) to "Evaluates Herdr compatibility as a
version floor plus three protocol tiers: below-minimum Error, reviewed Ok,
newer-than-reviewed tolerated." Replace the doc comment at line 629 (it
currently reads `/// Classifies legacy integer and newer date-era active
versions.`) with:

```rust
/// Classifies legacy integer and newer date-era active versions.
///
/// The date-era predicate is deliberately shape-based (two or more all-digit
/// dot-separated components, no floor comparison): herdr's date-era format is
/// not a contract this crate owns, a semantic predicate (4-digit year checks)
/// would risk false incompatibility on legitimate future forms, and a
/// wrong-shaped value classified as compatible only degrades a diagnostic,
/// never an enforcement path. Recorded as a deliberate choice during the
/// Increment 6 review.
```

- [ ] **Step 5: Implement `src/doctor.rs`.** Four changes:
  1. Code registry: add
     `"herdr_protocol_newer_unreviewed" => "Herdr protocol is newer than reviewed",`.
  2. Observation struct:

```rust
pub struct HerdrCompatibilityObservation {
    pub found_version: String,
    pub minimum_version: String,
    pub found_protocol: u32,
    pub minimum_protocol: u32,
    pub supported_protocols: Vec<u32>,
}
```

  3. In `herdr_compatibility_check`, the `normalized` binding is a SECOND
     exhaustive match over the enum — extend it:

```rust
    let normalized = match &assessment {
        HerdrCompatibility::Compatible { version } => Some(version.clone()),
        HerdrCompatibility::NewerUnreviewed { version } => Some(version.clone()),
        HerdrCompatibility::Unavailable { version, .. } => version.clone(),
    };
```

     and the observation constructor gains
     `minimum_protocol: remote::MINIMUM_HERDR_PROTOCOL,` with
     `supported_protocols: remote::REVIEWED_HERDR_PROTOCOLS.to_vec(),`.
  4. The assessment match gains the Warning arm:

```rust
        HerdrCompatibility::NewerUnreviewed { .. } => check(
            CheckStatus::Warning,
            "herdr_protocol_newer_unreviewed",
            observed,
        ),
```

- [ ] **Step 6: Run to verify green**

Run: `cargo test --locked --lib i4_remote_herdr_version_protocol_matrix`,
then `cargo test --locked --lib i7_reviewed_protocol_set_is_ascending_and_floored`,
then `cargo test --locked --lib i4_doctor_compatibility_matrix`,
then `cargo test --locked --test doctor`
Expected: PASS each.

- [ ] **Step 7: Live acceptance (spec criterion 4).** Build and run against
  the live herdr 0.8.2:

```bash
cargo build --locked
./target/debug/herdr-top doctor 2>&1 | grep -E 'compatibility\.herdr|herdr\.pong'
```

Expected: `compatibility.herdr: ok [herdr_compatible]` with observed
containing `"found_protocol":20,"minimum_protocol":19,"supported_protocols":[19,20]`.
Record the output in the task report.

- [ ] **Step 8: Run the full MSRV-local gate** (constraint 2). Expected: all
  pass.

- [ ] **Step 9: Commit**

```bash
git add src/diagnostics/remote.rs src/doctor.rs tests/doctor.rs
git commit -m "feat(doctor): three-tier herdr protocol compatibility with a newer-unreviewed warning"
```

---

### Task 2: Additive-tolerance pins — typed payloads and the event envelope

**Files:**
- Create: `tests/wire_tolerance.rs`
- Modify: `src/herdr/wire.rs` (the event-push envelope check at lines
  108-112 inside `EventStream::next_event`, plus any test in its `mod tests`
  at line 281+ that pins the exactly-two-keys behavior)

**Interfaces:**
- Consumes: `herdr_top::herdr::types::{Pong, Snapshot}` (existing), fixture
  `tests/fixtures/wire/p1-snapshot.jsonl` (existing; exactly one recv `pong`
  and one recv `session_snapshot`). `Subscription` is an outbound request
  type; the inbound surfaces are the typed payloads (Pong, Snapshot with its
  embedded Info structs) AND the event-push envelope parsed by hand in
  `EventStream::next_event`.
- Produces: the envelope tolerance the doctor Warning tier's
  "monitoring keeps running" rationale depends on. Spec component 4 records
  this relaxation explicitly.

- [ ] **Step 1: Write the failing envelope tests** in `src/herdr/wire.rs`'s
  existing `mod tests` (line 281+; follow that module's established pattern
  for driving `next_event` with an in-memory stream). Two cases:
  1. An event push carrying an extra key —
     `{"event":"pane.updated","data":{},"seq":7}` — must yield
     `Ok(Some(("pane.updated", data)))` (extra keys tolerated).
  2. An event push missing `data` — `{"event":"pane.updated"}` — must still
     fail with `WireError::MalformedFrame`.
  If an existing test in that module asserts the exactly-two-keys rejection,
  rewrite it into case 1's expectation instead of deleting it.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --locked --lib next_event`
Expected: the extra-key case FAILS against the current strict parser
(`event push must contain exactly event and data`).

- [ ] **Step 3: Relax the envelope check.** Replace lines 108-112:

```rust
        if !object.contains_key("event") || !object.contains_key("data") {
            return Err(WireError::MalformedFrame(
                "event push must contain event and data".into(),
            ));
        }
```

(Extra keys are now ignored; both required keys are still mandatory, and the
downstream `event`-must-be-a-string / `data`-present checks are unchanged.)

- [ ] **Step 4: Write the typed-payload tolerance test file**
  `tests/wire_tolerance.rs`:

```rust
//! Pins the additive tolerance the newer-protocol Warning tier relies on:
//! every inbound wire payload must deserialize when unknown fields appear at
//! any object level. A `deny_unknown_fields` regression fails this test.

use herdr_top::herdr::types::{Pong, Snapshot};
use serde_json::Value;

fn inject_unknown_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for child in map.values_mut() {
                inject_unknown_fields(child);
            }
            map.insert(
                "herdr_top_tolerance_probe".to_owned(),
                serde_json::json!({"future": [1, 2, 3]}),
            );
        }
        Value::Array(items) => {
            for item in items {
                inject_unknown_fields(item);
            }
        }
        _ => {}
    }
}

fn recv_results(kind: &str) -> Vec<Value> {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/wire/p1-snapshot.jsonl"
    ))
    .expect("wire fixture is readable");
    raw.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|entry| entry["dir"] == "recv")
        .filter_map(|entry| {
            let result = entry["payload"]["result"].clone();
            (result["type"] == kind).then_some(result)
        })
        .collect()
}

#[test]
fn i7_pong_tolerates_unknown_fields_at_every_level() {
    let payloads = recv_results("pong");
    assert!(!payloads.is_empty(), "fixture must contain pong results");
    for mut payload in payloads {
        inject_unknown_fields(&mut payload);
        serde_json::from_value::<Pong>(payload)
            .expect("pong deserializes with unknown fields injected");
    }
}

#[test]
fn i7_snapshot_tolerates_unknown_fields_at_every_level() {
    let payloads = recv_results("session_snapshot");
    assert!(
        !payloads.is_empty(),
        "fixture must contain session_snapshot results"
    );
    for mut payload in payloads {
        let mut snapshot = payload["snapshot"].clone();
        assert!(snapshot.is_object(), "snapshot payload is an object");
        inject_unknown_fields(&mut snapshot);
        serde_json::from_value::<Snapshot>(snapshot)
            .expect("snapshot deserializes with unknown fields injected");
    }
}
```

- [ ] **Step 5: Prove the typed-payload pin bites.** Temporarily add
  `#[serde(deny_unknown_fields)]` to `Pong` in `src/herdr/types.rs`, run
  `cargo test --locked --test wire_tolerance`, confirm the pong test FAILS,
  then revert the attribute.

- [ ] **Step 6: Run to verify green**

Run: `cargo test --locked --test wire_tolerance` and
`cargo test --locked --lib next_event`
Expected: all pass.

- [ ] **Step 7: Run the full MSRV-local gate** (constraint 2). Expected: all
  pass.

- [ ] **Step 8: Commit**

```bash
git add tests/wire_tolerance.rs src/herdr/wire.rs
git commit -m "feat(herdr): tolerate additive fields on every inbound wire surface"
```

---

### Task 3: Schema baseline and review script

**Files:**
- Create: `tests/fixtures/herdr-schema/baseline.json` (generated)
- Create: `tests/fixtures/herdr-schema/README.md`
- Create: `scripts/review-herdr-protocol.sh`
- Create: `tests/schema_review_script.rs`

**Interfaces:**
- Consumes: the installed herdr 0.8.2 binary (`~/.local/bin/herdr`), whose
  `herdr api schema --json` prints a JSON document with top-level keys
  `$schema`, `protocol` (= 20), `schema_version`, `schemas`, `title`
  (verified live; ~255 KB).
- Produces: the baseline consumed by every future reviewed-set extension;
  script contract `review-herdr-protocol.sh (--candidate-file SCHEMA_JSON |
  HERDR_BINARY)` with exit 0 = additive or identical, 1 = review required
  (removed or changed schema records, or an already-reviewed protocol whose
  canonicalized document differs from the baseline), and 2 = extraction,
  parse, or invalid-input failure.

- [ ] **Step 1: Generate the baseline**

```bash
mkdir -p tests/fixtures/herdr-schema
"$HOME/.local/bin/herdr" api schema --json > tests/fixtures/herdr-schema/baseline.json
"$HOME/.local/bin/herdr" --version && sha256sum "$HOME/.local/bin/herdr"
python3 -c "import json;d=json.load(open('tests/fixtures/herdr-schema/baseline.json'));assert d['protocol']==20,d['protocol']"
```

- [ ] **Step 2: Write `tests/fixtures/herdr-schema/README.md`** recording:
  what the fixture is (the bundled socket-API schema document — top-level
  keys `$schema`, `protocol`, `schema_version`, `schemas`, `title` — for the
  highest reviewed protocol, currently 20), source (herdr 0.8.2, the
  recorded `herdr --version` output and binary sha256 from step 1),
  extraction command (`herdr api schema --json`), and the extension
  procedure (run `scripts/review-herdr-protocol.sh` against the new herdr,
  review the printed delta, then in one change extend
  `REVIEWED_HERDR_PROTOCOLS`, replace `baseline.json`, and update this
  README — the baseline filename is version-agnostic so no other file
  changes).

- [ ] **Step 3: Write the failing script tests** `tests/schema_review_script.rs`:

```rust
//! Exercises scripts/review-herdr-protocol.sh against the committed baseline.

use std::process::Command;

const BASELINE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/herdr-schema/baseline.json"
);

fn run(args: &[&str]) -> std::process::Output {
    Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/review-herdr-protocol.sh"
        ))
        .args(args)
        .output()
        .expect("script runs")
}

fn temp_candidate(name: &str, value: &serde_json::Value) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "herdr-schema-review-{}-{name}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("candidate.json");
    std::fs::write(&path, value.to_string()).expect("write candidate");
    path
}

#[test]
fn i7_baseline_reviews_clean_against_itself() {
    let out = run(&["--candidate-file", BASELINE]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_removed_key_path_requires_review() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    let schemas = mutated["schemas"]
        .as_object_mut()
        .expect("schemas is an object");
    let first_key = schemas.keys().next().expect("non-empty").clone();
    schemas.remove(&first_key);
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("removed", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn i7_reviewed_protocol_value_drift_requires_review() {
    // Same protocol number, changed VALUE (no key-path delta): must exit 1.
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["title"] = serde_json::json!("drifted-title-value");
    let candidate = temp_candidate("drift", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn i7_unreadable_candidate_is_an_extraction_failure() {
    let out = run(&["--candidate-file", "/nonexistent/schema.json"]);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn i7_baseline_protocol_matches_the_reviewed_set_maximum() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let newest = *herdr_top::diagnostics::remote::REVIEWED_HERDR_PROTOCOLS
        .last()
        .expect("non-empty");
    assert_eq!(baseline["protocol"], serde_json::json!(newest));
}
```

(If `diagnostics::remote` is not reachable from integration tests, check
`src/diagnostics/mod.rs` for the `pub mod remote;` declaration — it is used
across the crate as `remote::...`; adjust the path, never the visibility,
unless the module is genuinely private, in which case widen it to `pub`.)

- [ ] **Step 4: Run to verify failure**

Run: `cargo test --locked --test schema_review_script`
Expected: FAIL (script does not exist).

- [ ] **Step 5: Write `scripts/review-herdr-protocol.sh`**

The canonical implementation is `scripts/review-herdr-protocol.sh`. This plan
intentionally does not duplicate its script body, so the plan and executable
cannot drift. The script accepts either `--candidate-file SCHEMA_JSON` or a
`HERDR_BINARY` path; the binary form runs `<binary> api schema --json`, and no
argument is an input error.

Before comparing, it exports `LC_ALL=C` so `sort` and `comm` use the same byte
collation (some UTF-8 locales make `comm` reject `sort`'s own output). It reads
the candidate and baseline protocols and requires both to match
`^[0-9]+$` before any Bash arithmetic comparison, failing closed on malformed
or attacker-controlled values.

The comparison unit is a schema record. For every JSON path, the script emits
`<path>` TAB `type:<jq type>`. It also emits `<path>` TAB `value:<json>` for
every non-array, non-object value except top-level `.protocol`, whose value is
compared explicitly so a legitimate protocol bump does not appear to remove a
record. For every array member that is itself an object or array, it emits
`<path>` TAB `member:<canonical json>` for the whole member. Record paths
render every array index as `[]`, so a member's position is not part of its
records; together with the sorted multiset comparison, this makes reordering
array members invisible. Recursive object-key sorting instead normalizes key
order inside each member's JSON, so differing key order does not create a
difference. Index elision can produce identical records, so every record in the
sorted stream receives a trailing `occurrence:N` suffix, numbered per distinct
record text (first instance `occurrence:1`, repeats `occurrence:2`, and so on),
and this per-record numbering makes the comparison a multiset comparison. Thus
removal of one of several identical records is detected while document record
order is ignored.

Added and removed records come from `comm -13` and `comm -23` over the sorted
multisets. The script prints `added schema records:` and
`removed schema records:` counts followed by each nonempty record list. A
displayed record longer than 200 characters is shortened to its first 200
characters plus `...`; comparison always uses the full record.

Exit 0 means additive or identical. Exit 1 means review is required because a
schema record was removed or changed, or because a candidate protocol less
than or equal to the baseline protocol has a different canonicalized whole
document. Exit 2 means extraction, parsing, or input validation failed,
including an unreadable candidate file, binary extraction failure, missing
baseline, invalid JSON, or a missing or non-integer protocol.

Then `chmod +x scripts/review-herdr-protocol.sh`.

- [ ] **Step 6: Run to verify green**

Run: `cargo test --locked --test schema_review_script`
Expected: 19 passed. Also run the live path once:
`scripts/review-herdr-protocol.sh "$HOME/.local/bin/herdr"` — expected
"additive or identical", exit 0.

- [ ] **Step 7: Run the full MSRV-local gate** (constraint 2). Expected: all
  pass.

- [ ] **Step 8: Commit**

```bash
git add tests/fixtures/herdr-schema/ scripts/review-herdr-protocol.sh tests/schema_review_script.rs
git commit -m "feat(diagnostics): commit the reviewed-schema baseline and review script"
```

---

### Task 4: Mechanical batch — featureless gate and hook identifier cap

**Files:**
- Modify: `tests/workload_harness.rs` (the ungated `#[test]` at line 6998,
  `fn section15_selected_paths_reject_absolute_noncanonical_spelling` at
  6999, whose call to the feature-gated `validate_section15_shape_for_test`
  at line 7009 breaks featureless `cargo check --all-targets` with E0425)
- Modify: `.github/workflows/ci.yml` (add a featureless check step so the
  fix has an ongoing guard)
- Modify: `src/hook_adapter.rs` (payload struct at lines 10-23,
  `map_hook_payload` at line 25, test-module imports at line 159)
- Modify: `src/main.rs` (`run_emit` at lines 196+; the invalid-payload arm
  warns to stderr and returns `ExitCode::SUCCESS`; `read_hook_payload()` is
  at line 293; `map_hook_payload` is called near line 213)
- Modify: `tests/controller.rs` (CLI-level enforcement test; the existing
  from-hook invocation pattern is at lines 605-607:
  `command.args(["--session", SESSION, "emit", "--from-hook", provider])`)

**Interfaces:**
- Consumes: existing `HookPayload` (`session_id: String`,
  `agent_id: Option<String>`, `task_id: Option<String>`), existing emit
  invalid-payload policy in `run_emit`.
- Produces: `pub const HOOK_IDENTIFIER_MAX_BYTES: usize = 128` and
  `pub fn validate_hook_identifiers(payload: &HookPayload) -> Result<(), String>`
  in `hook_adapter`.

- [ ] **Step 1: Gate the featureless-broken test.** Add directly above the
  `#[test]` attribute at line 6998:

```rust
#[cfg(feature = "workload-harness")]
```

Do NOT delete or weaken the test: it is one of the two pins backing the
recorded reply to a Copilot finding on PR #6.

- [ ] **Step 2: Verify both build modes**

Run: `cargo check --locked --all-targets` (featureless)
Expected: PASS (E0425 gone; it currently fails at tests/workload_harness.rs:7009).
Run: `cargo test --locked --all-features --test workload_harness section15_selected_paths_reject_absolute_noncanonical_spelling`
Expected: 1 passed.

- [ ] **Step 3: Guard it in CI.** In `.github/workflows/ci.yml`, add to the
  lint job, after the clippy step:

```yaml
      - name: Featureless check (no default or optional features)
        run: cargo +stable check --locked --all-targets
```

(Match the surrounding step style; the job already installs the stable
toolchain.)

- [ ] **Step 4: Write the failing cap tests** in the `hook_adapter` test
  module. First extend the import list at line 159 to
  `use super::{HOOK_IDENTIFIER_MAX_BYTES, HookPayload, HookProvider, map_hook_payload, validate_hook_identifiers};`
  then add:

```rust
    fn payload_with(session: &str, agent: Option<&str>, task: Option<&str>) -> HookPayload {
        HookPayload {
            hook_event_name: "SessionStart".to_owned(),
            session_id: session.to_owned(),
            source: None,
            agent_id: agent.map(str::to_owned),
            agent_type: None,
            task_id: task.map(str::to_owned),
            task_subject: None,
        }
    }

    #[test]
    fn i7_identifiers_at_the_cap_are_accepted() {
        let max = "a".repeat(HOOK_IDENTIFIER_MAX_BYTES);
        let payload = payload_with(&max, Some(&max), Some(&max));
        assert_eq!(validate_hook_identifiers(&payload), Ok(()));
    }

    #[test]
    fn i7_oversized_identifiers_are_rejected_per_field() {
        let over = "a".repeat(HOOK_IDENTIFIER_MAX_BYTES + 1);
        let session = payload_with(&over, None, None);
        let error = validate_hook_identifiers(&session).unwrap_err();
        assert!(error.contains("session_id"), "{error}");
        assert!(error.contains("129"), "{error}");
        assert!(!error.contains(&over), "must not echo the identifier");

        let agent = payload_with("s", Some(&over), None);
        assert!(validate_hook_identifiers(&agent)
            .unwrap_err()
            .contains("agent_id"));

        let task = payload_with("s", None, Some(&over));
        assert!(validate_hook_identifiers(&task)
            .unwrap_err()
            .contains("task_id"));
    }
```

- [ ] **Step 5: Run to verify failure**

Run: `cargo test --locked --lib i7_identifiers_at_the_cap_are_accepted`
Expected: compile failure (`HOOK_IDENTIFIER_MAX_BYTES`,
`validate_hook_identifiers` unresolved).

- [ ] **Step 6: Implement** in `src/hook_adapter.rs`:

```rust
/// Longest accepted hook-provided identifier, in bytes. Observed provider
/// identifiers (UUIDs, prefixed hex ids) stay far below this; the cap bounds
/// run-id, event-id, and log growth from a misbehaving hook caller.
pub const HOOK_IDENTIFIER_MAX_BYTES: usize = 128;

/// Rejects hook payloads whose identifiers exceed the byte cap. The error
/// names the field and length but never echoes identifier content.
pub fn validate_hook_identifiers(payload: &HookPayload) -> Result<(), String> {
    let fields = [
        ("session_id", Some(payload.session_id.as_str())),
        ("agent_id", payload.agent_id.as_deref()),
        ("task_id", payload.task_id.as_deref()),
    ];
    for (name, value) in fields {
        if let Some(value) = value {
            if value.len() > HOOK_IDENTIFIER_MAX_BYTES {
                return Err(format!(
                    "hook {name} is {} bytes, exceeding the {HOOK_IDENTIFIER_MAX_BYTES}-byte cap",
                    value.len()
                ));
            }
        }
    }
    Ok(())
}
```

In `src/main.rs` `run_emit`, immediately after the successful
`read_hook_payload()` match and before `map_hook_payload` is called:

```rust
        if let Err(reason) = hook_adapter::validate_hook_identifiers(&payload) {
            eprintln!("herdr-top emit: warning: {reason}; ignored");
            return ExitCode::SUCCESS;
        }
```

- [ ] **Step 7: Write the CLI-level enforcement test** in
  `tests/controller.rs`, mirroring the nearest existing from-hook test's
  setup (binary invocation, stdin payload, delivery assertion helpers): feed
  a hook payload whose `session_id` is 129 bytes, assert (1) exit status
  success, (2) stderr contains `exceeding the 128-byte cap`, (3) no envelope
  is delivered (the same no-delivery assertion the neighboring tests use).
  This is the regression pin for the `run_emit` hookup — without it, a
  refactor could drop the call while every unit test stays green.

- [ ] **Step 8: Run to verify green**

Run: `cargo test --locked --lib i7_identifiers_at_the_cap_are_accepted`,
then `cargo test --locked --lib i7_oversized_identifiers_are_rejected_per_field`,
then `cargo test --locked --test controller`
Expected: PASS each.

- [ ] **Step 9: Run the full MSRV-local gate** (constraint 2) plus
  `cargo check --locked --all-targets`. Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add tests/workload_harness.rs .github/workflows/ci.yml src/hook_adapter.rs src/main.rs tests/controller.rs
git commit -m "fix(emit): cap hook identifiers and gate the featureless-broken harness test"
```

---

### Task 5: Plugin manifest, fetch script, pins, and release guide

**Files:**
- Create: `herdr-plugin.toml`
- Create: `scripts/fetch-release.sh`
- Create: `scripts/release-pins.env`
- Create: `tests/fetch_release_script.rs`
- Create: `docs/guides/release-process.md`
- Modify: `.gitignore` (add `/bin/` — the fetch script and the documented
  dev flow both place a binary at `bin/herdr-top` in a checkout)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: contracts Task 6's workflow smoke uses —
  `HERDR_TOP_FETCH_LOCAL_DIR` (directory holding the archive and a
  `SHA256SUMS`), `HERDR_TOP_FETCH_LOCAL_VERSION`, and
  `HERDR_TOP_FETCH_PINS_FILE` (overrides the pins path; tests use it so the
  suite stays hermetic after real pins land); archive naming
  `herdr-top-<version>-<target>.tar.gz`; targets exactly
  `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`,
  `aarch64-unknown-linux-gnu`.

- [ ] **Step 1: Add `herdr-plugin.toml`** byte-for-byte from design section 12:

```toml
id = "mageyuki.herdr-top"
name = "Herdr Top"
version = "0.1.0"
min_herdr_version = "0.8.0"
platforms = ["linux", "macos"]

[[build]]
command = ["scripts/fetch-release.sh"]

[[panes]]
id = "top"
title = "Herdr Top"
placement = "tab"
command = ["bin/herdr-top"]
```

- [ ] **Step 2: Add `scripts/release-pins.env`** (empty sentinel state; the
  release tail's pin commit fills it) and the `.gitignore` entry `/bin/`:

```bash
# Release pins consumed by scripts/fetch-release.sh.
# Filled by the pin commit that follows each published release; see
# docs/guides/release-process.md. Empty version = no release pinned yet.
HERDR_TOP_RELEASE_VERSION=""
HERDR_TOP_SHA256_AARCH64_APPLE_DARWIN=""
HERDR_TOP_SHA256_X86_64_APPLE_DARWIN=""
HERDR_TOP_SHA256_X86_64_UNKNOWN_LINUX_GNU=""
HERDR_TOP_SHA256_AARCH64_UNKNOWN_LINUX_GNU=""
```

- [ ] **Step 3: Write the failing script tests** `tests/fetch_release_script.rs`.
  The no-pins test MUST use a synthetic pins file via
  `HERDR_TOP_FETCH_PINS_FILE` — never the repository's live
  `release-pins.env`, which the Phase R pin commit later fills (pointing at
  the live file would make the test flip to a network-dependent failure the
  moment real pins land):

```rust
//! Exercises scripts/fetch-release.sh in local-source and no-pins modes.

use std::process::Command;

struct LocalFixture {
    dir: std::path::PathBuf,
    workdir: std::path::PathBuf,
}

fn make_fixture(corrupt: bool) -> LocalFixture {
    let base = std::env::temp_dir().join(format!(
        "herdr-top-fetch-{}-{corrupt}",
        std::process::id()
    ));
    let dir = base.join("artifacts");
    let workdir = base.join("plugin");
    std::fs::create_dir_all(&dir).expect("artifact dir");
    std::fs::create_dir_all(&workdir).expect("plugin dir");
    let staging = base.join("stage");
    std::fs::create_dir_all(&staging).expect("stage dir");
    std::fs::write(staging.join("herdr-top"), b"#!/bin/sh\necho stub-0.1.0\n")
        .expect("stub binary");
    let target = current_target();
    let archive = dir.join(format!("herdr-top-0.1.0-{target}.tar.gz"));
    let tar = Command::new("tar")
        .args(["-czf"])
        .arg(&archive)
        .args(["-C"])
        .arg(&staging)
        .arg("herdr-top")
        .status()
        .expect("tar runs");
    assert!(tar.success());
    let bytes = std::fs::read(&archive).expect("archive bytes");
    let digest = sha256_hex(&bytes);
    let sums = if corrupt {
        format!("{:0>64}  herdr-top-0.1.0-{target}.tar.gz\n", "0")
    } else {
        format!("{digest}  herdr-top-0.1.0-{target}.tar.gz\n")
    };
    std::fs::write(dir.join("SHA256SUMS"), sums).expect("sums file");
    LocalFixture { dir, workdir }
}

fn current_target() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        other => panic!("unsupported test platform: {other:?}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let out = Command::new("sh")
        .arg("-c")
        .arg("sha256sum 2>/dev/null || shasum -a 256")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("stdin")
                .write_all(bytes)
                .expect("write");
            child.wait_with_output()
        })
        .expect("digest tool runs");
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .expect("digest")
        .to_owned()
}

fn run_fetch(fixture: &LocalFixture) -> std::process::Output {
    Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/fetch-release.sh"
        ))
        .current_dir(&fixture.workdir)
        .env("HERDR_TOP_FETCH_LOCAL_DIR", &fixture.dir)
        .env("HERDR_TOP_FETCH_LOCAL_VERSION", "0.1.0")
        .output()
        .expect("script runs")
}

#[test]
fn i7_local_source_install_places_executable_binary() {
    let fixture = make_fixture(false);
    let out = run_fetch(&fixture);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let installed = fixture.workdir.join("bin/herdr-top");
    assert!(installed.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = installed.metadata().expect("metadata").permissions().mode();
        assert_ne!(mode & 0o111, 0, "binary must be executable");
    }
}

#[test]
fn i7_checksum_mismatch_fails_and_installs_nothing_and_keeps_the_source() {
    let fixture = make_fixture(true);
    let out = run_fetch(&fixture);
    assert_ne!(out.status.code(), Some(0));
    assert!(!fixture.workdir.join("bin/herdr-top").exists());
    let target = current_target();
    assert!(
        fixture
            .dir
            .join(format!("herdr-top-0.1.0-{target}.tar.gz"))
            .exists(),
        "local-mode source archives must never be deleted"
    );
}

#[test]
fn i7_without_pins_and_without_local_source_the_script_fails_closed() {
    let base = std::env::temp_dir().join(format!("herdr-top-nopin-{}", std::process::id()));
    std::fs::create_dir_all(&base).expect("dir");
    let empty_pins = base.join("release-pins.env");
    std::fs::write(
        &empty_pins,
        "HERDR_TOP_RELEASE_VERSION=\"\"\n",
    )
    .expect("pins file");
    let out = Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/fetch-release.sh"
        ))
        .current_dir(&base)
        .env("HERDR_TOP_FETCH_PINS_FILE", &empty_pins)
        .output()
        .expect("script runs");
    assert_ne!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stderr).contains("no release pinned"));
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test --locked --test fetch_release_script`
Expected: FAIL (script does not exist).

- [ ] **Step 5: Write `scripts/fetch-release.sh`**

```bash
#!/usr/bin/env bash
# herdr plugin [[build]] command: fetches the pinned release artifact for the
# current platform, verifies its sha256 against repo-committed pins, and
# installs bin/herdr-top. Requires no Rust toolchain. Never touches PATH or
# /dev/tty. See docs/guides/release-process.md for the pin lifecycle.
#
# CI-only local-source mode: HERDR_TOP_FETCH_LOCAL_DIR (holding the archive
# and SHA256SUMS) + HERDR_TOP_FETCH_LOCAL_VERSION verify a just-built archive
# without any network access. HERDR_TOP_FETCH_PINS_FILE overrides the pins
# path (tests use it to stay hermetic).
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

fail() { printf 'fetch-release: error: %s\n' "$1" >&2; exit 1; }

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64) target=x86_64-unknown-linux-gnu; pin_var=HERDR_TOP_SHA256_X86_64_UNKNOWN_LINUX_GNU ;;
  Linux/aarch64) target=aarch64-unknown-linux-gnu; pin_var=HERDR_TOP_SHA256_AARCH64_UNKNOWN_LINUX_GNU ;;
  Darwin/x86_64) target=x86_64-apple-darwin; pin_var=HERDR_TOP_SHA256_X86_64_APPLE_DARWIN ;;
  Darwin/arm64) target=aarch64-apple-darwin; pin_var=HERDR_TOP_SHA256_AARCH64_APPLE_DARWIN ;;
  *) fail "unsupported platform: $(uname -s)/$(uname -m)" ;;
esac

digest_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    fail "no sha256 tool available"
  fi
}

workdir=$PWD
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

downloaded=""
if [[ -n ${HERDR_TOP_FETCH_LOCAL_DIR-} ]]; then
  version=${HERDR_TOP_FETCH_LOCAL_VERSION:?local mode needs HERDR_TOP_FETCH_LOCAL_VERSION}
  archive_name="herdr-top-$version-$target.tar.gz"
  archive="$HERDR_TOP_FETCH_LOCAL_DIR/$archive_name"
  [[ -f $archive ]] || fail "local archive missing: $archive_name"
  expected=$(awk -v name="$archive_name" '$2 == name {print $1}' \
    "$HERDR_TOP_FETCH_LOCAL_DIR/SHA256SUMS")
  [[ -n $expected ]] || fail "no SHA256SUMS entry for $archive_name"
else
  pins_file=${HERDR_TOP_FETCH_PINS_FILE:-"$script_dir/release-pins.env"}
  # shellcheck source=release-pins.env
  source "$pins_file"
  version=${HERDR_TOP_RELEASE_VERSION-}
  [[ -n $version ]] || fail "no release pinned yet (release pins are empty)"
  expected=${!pin_var-}
  [[ -n $expected ]] || fail "no checksum pinned for $target"
  archive_name="herdr-top-$version-$target.tar.gz"
  archive="$tmpdir/$archive_name"
  url="https://github.com/mageyuki/herdr-top/releases/download/v$version/$archive_name"
  curl --fail --location --silent --show-error --retry 3 --retry-delay 2 \
    --output "$archive" "$url" || fail "download failed: $url"
  downloaded="$archive"
fi

actual=$(digest_of "$archive")
if [[ $actual != "$expected" ]]; then
  # Remove only a bad DOWNLOAD; a local-mode source archive belongs to the
  # caller (in CI it is the build output staged for upload).
  [[ -z $downloaded ]] || rm -f "$downloaded"
  fail "checksum mismatch for $archive_name (expected $expected, got $actual)"
fi

mkdir -p "$workdir/bin"
tar -xzf "$archive" -C "$tmpdir" herdr-top
install -m 0755 "$tmpdir/herdr-top" "$workdir/bin/herdr-top"
printf 'fetch-release: installed bin/herdr-top (%s, %s)\n' "$version" "$target"
```

Then `chmod +x scripts/fetch-release.sh`.

- [ ] **Step 6: Run to verify green**

Run: `cargo test --locked --test fetch_release_script`
Expected: 3 passed.

- [ ] **Step 7: Write `docs/guides/release-process.md`** covering, in order:
  what a release consists of (four archives + `SHA256SUMS` on a `v<version>`
  tag); the actor split (tag push, release publication, and promotion are
  user actions; everything else is automated or PR-driven); the mandatory
  `workflow_dispatch` dry run on the default branch BEFORE the first tag of
  each release (validates all four runner legs and the packaging/smoke path
  without creating anything irreversible); the pin lifecycle (tag ->
  workflow builds draft release -> user publishes as pre-release -> pin
  commit updates `scripts/release-pins.env` from the published `SHA256SUMS`
  via the normal PR flow -> managed install validation on Linux); the
  explicit consequence that `herdr plugin install` fails with "no release
  pinned yet" until the first pin commit lands; the version-skew guard (tag
  version must equal the `Cargo.toml` version); and the Marketplace topic
  step deferred until the user declares a release usable.

- [ ] **Step 8: Run the full MSRV-local gate** (constraint 2). Expected: all
  pass.

- [ ] **Step 9: Commit**

```bash
git add herdr-plugin.toml scripts/fetch-release.sh scripts/release-pins.env tests/fetch_release_script.rs docs/guides/release-process.md .gitignore
git commit -m "feat(packaging): add the plugin manifest, pinned fetch script, and release guide"
```

---

### Task 6: Release workflow

Integrate AFTER Task 5 (consumes its local-source contract and archive
naming; the smoke step fails if Task 5 is absent).

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: Task 5's `HERDR_TOP_FETCH_LOCAL_DIR` +
  `HERDR_TOP_FETCH_LOCAL_VERSION` contract and archive naming.
- Produces: draft releases with `herdr-top-<version>-<target>.tar.gz` times
  four plus `SHA256SUMS`.

- [ ] **Step 1: Resolve and pin the action SHAs.** The repository pins
  actions by commit SHA with `persist-credentials: false`
  (see `.github/workflows/ci.yml`, which uses
  `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`).
  Reuse that exact checkout pin. For the artifact actions resolve the
  current v4 tag SHAs once and record them:

```bash
gh api repos/actions/upload-artifact/git/ref/tags/v4 --jq '.object.sha'
gh api repos/actions/download-artifact/git/ref/tags/v4 --jq '.object.sha'
```

(If a returned object is an annotated tag, dereference it with
`gh api repos/actions/<repo>/git/tags/<sha> --jq '.object.sha'`.) Verify the
pinned download-artifact version still declares the `merge-multiple` input
(`gh api repos/actions/download-artifact/contents/action.yml?ref=<sha>`).

- [ ] **Step 2: Write `.github/workflows/release.yml`** (reproduce the block
  exactly, replacing only the three `<PINNED-*-SHA>` placeholders with the
  values from Step 1):

```yaml
name: release

on:
  push:
    tags: ["v*"]
  workflow_dispatch: {}

permissions:
  contents: read

jobs:
  build:
    strategy:
      fail-fast: true
      matrix:
        include:
          - target: aarch64-apple-darwin
            runner: macos-15
          - target: x86_64-apple-darwin
            runner: macos-15-intel
          - target: x86_64-unknown-linux-gnu
            runner: ubuntu-24.04
          - target: aarch64-unknown-linux-gnu
            runner: ubuntu-24.04-arm
    runs-on: ${{ matrix.runner }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
      - name: Install toolchain
        run: |
          rustup toolchain install stable --profile minimal
          rustup target add --toolchain stable ${{ matrix.target }}
      - name: Resolve version
        id: version
        run: |
          version=$(cargo +stable metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
          if [[ "${GITHUB_REF_TYPE}" == "tag" ]]; then
            tag_version="${GITHUB_REF_NAME#v}"
            if [[ "$tag_version" != "$version" ]]; then
              echo "tag $GITHUB_REF_NAME does not match Cargo.toml version $version" >&2
              exit 1
            fi
          fi
          echo "version=$version" >> "$GITHUB_OUTPUT"
      - name: Build
        run: cargo +stable build --release --locked --target ${{ matrix.target }}
      - name: Package
        run: |
          staging=$(mktemp -d)
          cp "target/${{ matrix.target }}/release/herdr-top" "$staging/"
          members=(herdr-top)
          for extra in LICENSE README.md; do
            if [[ -f "$extra" ]]; then
              cp "$extra" "$staging/" && members+=("$extra")
            fi
          done
          archive="herdr-top-${{ steps.version.outputs.version }}-${{ matrix.target }}.tar.gz"
          tar -czf "$archive" -C "$staging" "${members[@]}"
          mkdir -p dist && mv "$archive" dist/
      - name: Install smoke (local-source fetch)
        run: |
          cd dist
          if command -v sha256sum >/dev/null 2>&1; then
            sha256sum ./*.tar.gz > SHA256SUMS
          else
            shasum -a 256 ./*.tar.gz > SHA256SUMS
          fi
          sed -i.bak 's#\./##' SHA256SUMS && rm -f SHA256SUMS.bak
          cd ..
          smoke=$(mktemp -d)
          (cd "$smoke" && \
            HERDR_TOP_FETCH_LOCAL_DIR="$GITHUB_WORKSPACE/dist" \
            HERDR_TOP_FETCH_LOCAL_VERSION="${{ steps.version.outputs.version }}" \
            bash "$GITHUB_WORKSPACE/scripts/fetch-release.sh")
          "$smoke/bin/herdr-top" --version
      - uses: actions/upload-artifact@<PINNED-UPLOAD-SHA> # v4
        with:
          name: ${{ matrix.target }}
          path: dist/*.tar.gz
          if-no-files-found: error

  release:
    needs: build
    runs-on: ubuntu-24.04
    permissions:
      contents: write
    steps:
      - uses: actions/download-artifact@<PINNED-DOWNLOAD-SHA> # v4
        with:
          path: artifacts
          merge-multiple: true
      - name: Checksums
        run: |
          cd artifacts
          sha256sum ./*.tar.gz > SHA256SUMS
          sed -i 's#\./##' SHA256SUMS
          cat SHA256SUMS
      - uses: actions/upload-artifact@<PINNED-UPLOAD-SHA> # v4
        with:
          name: SHA256SUMS
          path: artifacts/SHA256SUMS
      - name: Draft release (tags only)
        if: github.event_name == 'push' && github.ref_type == 'tag'
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          gh release create "$GITHUB_REF_NAME" \
            --repo "$GITHUB_REPOSITORY" \
            --draft \
            --title "$GITHUB_REF_NAME" \
            --verify-tag \
            artifacts/*.tar.gz artifacts/SHA256SUMS
```

Design notes anchored in verified facts: the repository is public, so the
`ubuntu-24.04-arm` hosted runner is available; `macos-15` is an arm64 image
and the x64 image is `macos-15-intel`, so each Apple leg builds and smokes
NATIVELY (no cross-compilation, no Rosetta dependency); `--verify-tag`
prevents releasing an unpushed tag; the draft-release step requires both a
`push` event and a tag ref, so `workflow_dispatch` never creates a release,
even when dispatched against a tag ref (for example,
`gh workflow run release --ref v0.1.1`); write permission is scoped to the
release job. The per-leg smoke verifies packaging/extraction against a
self-computed checksum, so it is NOT checksum-path validation — the unit test
`i7_checksum_mismatch_fails_and_installs_nothing_and_keeps_the_source`
covers that. The `macos-15-intel` label is exercised for the first time by
the Phase R dry run; if GitHub rejects it, stop and re-plan that leg rather
than silently switching images.

- [ ] **Step 3: Static validation.** Probe actionlint with
  `actionlint -version` (NOT `command -v` — a mise shim exists but is
  inactive and fails at run time); if it works, run
  `actionlint .github/workflows/release.yml`; otherwise validate YAML
  syntax:
  `python3 -c "import yaml,sys;yaml.safe_load(open('.github/workflows/release.yml'))"`.
Expected: no errors. (The real integration test is the Phase R dry run —
hosted runners cannot be exercised locally.)

- [ ] **Step 4: Run the full MSRV-local gate** (constraint 2; the workflow
  file does not affect it, this guards accidental collateral edits).
  Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): build, verify, and draft tag-driven release artifacts"
```

---

### Task 7: Design-doc updates

**Files:**
- Modify: `docs/design/herdr-top-mvp.md` (platform row at line 18; doctor
  description around line 574; section 12.2 around line 549; ADR candidate
  list around line 766)

**Interfaces:**
- Consumes: final wording of Task 1/2/3/5 semantics and file names.
- Produces: nothing consumed by code.

- [ ] **Step 1: Update the platform row (line 18).** Replace the protocol
  clause introduced by PR #7 so the row reads:

```
| Required platform | Herdr 0.8.0 or newer; initial development and test baseline: Herdr 0.8.0, socket protocol 19; protocol compatibility is three-tiered: below 19 is a doctor Error, the reviewed set {19, 20} is compatible, newer than the reviewed set is a doctor Warning |
```

- [ ] **Step 2: Extend the doctor description** (the paragraph beginning
  "`doctor` checks Herdr socket" around line 574): after the sentence
  describing the Herdr compatibility check, add:

```
Protocol compatibility is three-tiered: protocols below the minimum are an
Error (`herdr_protocol_mismatch`), protocols in the reviewed set are
compatible, and protocols newer than every reviewed one are a Warning
(`herdr_protocol_newer_unreviewed`) — monitoring continues because every
inbound wire surface, the event-push envelope included, tolerates additive
fields. The reviewed set is extended only through
`scripts/review-herdr-protocol.sh`, which diffs a candidate herdr's bundled
schema against the committed baseline in `tests/fixtures/herdr-schema/`. No
newer-protocol socket feature is used anywhere; any future use must be gated
on the handshake protocol at the call site.
```

- [ ] **Step 3: Extend section 12.2** with one sentence after the
  checksum-verified binaries sentence:

```
Artifact checksums are pinned in `scripts/release-pins.env` by a follow-up
commit after each published release, so the build command trusts repository
content rather than the download source; the release procedure is documented
in `docs/guides/release-process.md`.
```

- [ ] **Step 4: Add the ADR candidate** to the list around line 766: change
  the current final item's terminal period
  (`- release binary and plugin installation strategy.`) to a semicolon and
  append as the new final item:

```
- static linking (musl) for Linux release artifacts.
```

- [ ] **Step 5: Verify and commit.** Re-read the four hunks in context
  (`git diff docs/design/herdr-top-mvp.md`), confirm no other line changed,
  then:

```bash
git add docs/design/herdr-top-mvp.md
git commit -m "docs(design): record three-tier protocol semantics and release pinning"
```

---

### Phase R: Release tail (after the increment PR merges)

Not implementation tasks; run in order, recording evidence in the increment
ledger. Steps 3 and 5 are user actions.

1. Confirm the merged main contains Tasks 1-7 and CI is green.
2. **Dry run first (spec acceptance criterion 1):** trigger the release
   workflow via `workflow_dispatch` on the default branch
   (`gh workflow run release --ref main`) and wait for it: all four legs
   (including the first-ever `macos-15-intel` and `ubuntu-24.04-arm`
   executions) and the aggregate job must succeed, with every leg's install
   smoke passing. Any failure is fixed through the normal PR flow BEFORE any
   tag exists — nothing irreversible has happened yet.
3. USER pushes the tag: `git tag v0.1.0 && git push origin v0.1.0`.
4. Watch the tag build; all four legs and the aggregate job must succeed and
   the draft release must appear with four archives plus `SHA256SUMS`.
5. USER publishes the draft release as a pre-release.
6. Pin PR: download the published `SHA256SUMS`, fill
   `scripts/release-pins.env` (version `0.1.0` plus the four digests), and
   publish through the normal PR flow.
7. After the pin PR merges: on Linux against the live herdr, run
   `herdr plugin install mageyuki/herdr-top`, confirm the fetch, the
   checksum verification, `bin/herdr-top --version` = 0.1.0, and that the
   plugin pane launches. Record command output.
8. Promotion of the pre-release and the `herdr-plugin` Marketplace topic
   remain user decisions, outside this increment.
