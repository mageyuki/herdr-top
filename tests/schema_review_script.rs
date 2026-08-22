//! Exercises scripts/review-herdr-protocol.sh against the committed baseline.

use std::process::Command;

const BASELINE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/herdr-schema/baseline.json"
);

fn run(args: &[&str]) -> std::process::Output {
    run_with_env(args, &[])
}

fn run_with_env(args: &[&str], envs: &[(&str, &str)]) -> std::process::Output {
    Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/review-herdr-protocol.sh"
        ))
        .args(args)
        .envs(envs.iter().copied())
        .output()
        .expect("script runs")
}

fn temp_candidate(name: &str, value: &serde_json::Value) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("herdr-schema-review-{}-{name}", std::process::id()));
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
fn i7_review_is_locale_independent() {
    // Guards the collation regression. Without ja_JP.UTF-8, glibc falls back to C,
    // so this degrades to a duplicate of the clean case instead of failing spuriously.
    let out = run_with_env(
        &["--candidate-file", BASELINE],
        &[("LC_ALL", "ja_JP.UTF-8"), ("LANG", "ja_JP.UTF-8")],
    );
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
