//! Exercises scripts/review-herdr-protocol.sh against the committed baseline.

use std::process::Command;

const BASELINE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/herdr-schema/baseline.json"
);
const PROVIDER_LOG_FIXTURES: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/provider-logs");

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

fn temp_provider_log_fixtures() -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("temp provider-log directory");
    for entry in std::fs::read_dir(PROVIDER_LOG_FIXTURES).expect("read provider-log fixtures") {
        let entry = entry.expect("provider-log fixture entry");
        if entry
            .file_type()
            .expect("provider-log fixture type")
            .is_file()
        {
            std::fs::copy(entry.path(), directory.path().join(entry.file_name()))
                .expect("copy provider-log fixture");
        }
    }
    directory
}

fn mutate_jsonl(path: &std::path::Path, mut mutator: impl FnMut(&mut serde_json::Value)) {
    let contents = std::fs::read_to_string(path).expect("read JSONL fixture");
    let mut output = String::new();
    for line in contents.lines() {
        let mut record: serde_json::Value =
            serde_json::from_str(line).expect("JSONL record parses");
        mutator(&mut record);
        output.push_str(&record.to_string());
        output.push('\n');
    }
    std::fs::write(path, output).expect("rewrite JSONL fixture");
}

#[test]
fn log_baselines_cover_fixture_record_types() {
    let out = run(&["--log-baselines", PROVIDER_LOG_FIXTURES]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("verdict: provider log baselines match"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn log_baseline_detects_novel_record_type() {
    use std::io::Write as _;

    let fixtures = temp_provider_log_fixtures();
    let mut transcript = std::fs::OpenOptions::new()
        .append(true)
        .open(fixtures.path().join("claude-session.jsonl"))
        .expect("open copied Claude transcript");
    writeln!(
        transcript,
        r#"{{"type":"future-provider-record","timestamp":"2026-08-24T06:00:00.000Z","sessionId":"13f03635-c1f6-46e2-8e52-83d217b6f01c","cwd":"/home/user/git/example/herdr-top","version":"2.1.239"}}"#
    )
    .expect("append novel record");

    let out = run(&[
        "--log-baselines",
        fixtures.path().to_str().expect("UTF-8 fixture path"),
    ]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("future-provider-record"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn log_baseline_rejects_non_string_claude_version() {
    let fixtures = temp_provider_log_fixtures();
    mutate_jsonl(&fixtures.path().join("claude-subagent.jsonl"), |record| {
        if record["type"] == "user" {
            record["version"] = serde_json::json!(2_102_390);
        }
    });

    let out = run(&[
        "--log-baselines",
        fixtures.path().to_str().expect("UTF-8 fixture path"),
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr)
            .contains("error: Claude transcript version must be a string"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn log_baseline_rejects_non_string_codex_cli_version() {
    let fixtures = temp_provider_log_fixtures();
    mutate_jsonl(
        &fixtures.path().join("codex-exec-resume-appended.jsonl"),
        |record| {
            if record["type"] == "session_meta" {
                record["payload"]["cli_version"] = serde_json::json!(149_000);
            }
        },
    );

    let out = run(&[
        "--log-baselines",
        fixtures.path().to_str().expect("UTF-8 fixture path"),
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("error: Codex cli_version must be a string"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn log_baseline_requires_version_in_every_claude_file() {
    let fixtures = temp_provider_log_fixtures();
    mutate_jsonl(&fixtures.path().join("claude-subagent.jsonl"), |record| {
        record
            .as_object_mut()
            .expect("Claude record is an object")
            .remove("version");
    });

    let out = run(&[
        "--log-baselines",
        fixtures.path().to_str().expect("UTF-8 fixture path"),
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr)
            .contains("error: no Claude transcript version found in claude-subagent.jsonl"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn log_baseline_renders_version_mismatch_columns_with_tabs() {
    let fixtures = temp_provider_log_fixtures();
    mutate_jsonl(&fixtures.path().join("claude-subagent.jsonl"), |record| {
        if record.get("version").is_some() {
            record["version"] = serde_json::json!("2.2.0");
        }
    });

    let out = run(&[
        "--log-baselines",
        fixtures.path().to_str().expect("UTF-8 fixture path"),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {stdout} stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("claude\tversion:2.2.0\texpected-prefix:2.1."),
        "stdout: {stdout} stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !stdout.contains(r"claude\tversion:"),
        "stdout: {stdout} stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
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
fn i7_protocol_bump_only_reviews_clean() {
    let mut candidate: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    candidate["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("protocol-bump-only", &candidate);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("verdict: additive or identical"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_live_binary_invocation_reviews_clean_when_available() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::unix::fs::PermissionsExt;

    let Some(binary) = std::env::var_os("HERDR_BINARY")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(|home| std::path::PathBuf::from(home).join(".local/bin/herdr"))
        })
    else {
        println!("skipping live herdr schema review: neither HERDR_BINARY nor HOME is set");
        return Ok(());
    };
    let Ok(metadata) = std::fs::metadata(&binary) else {
        println!(
            "skipping live herdr schema review: {} is absent",
            binary.display()
        );
        return Ok(());
    };
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        println!(
            "skipping live herdr schema review: {} is not executable",
            binary.display()
        );
        return Ok(());
    }
    let Some(binary) = binary.to_str() else {
        println!("skipping live herdr schema review: binary path is not UTF-8");
        return Ok(());
    };

    // A future herdr schema that is not additive should fail here as intended signal.
    let out = run(&[binary]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
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
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_removed_object_array_member_requires_review() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["event"]["$defs"]["EventData"]["oneOf"]
        .as_array_mut()
        .expect("oneOf is an array")
        .remove(0);
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("object-member-removal", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_one_duplicate_removed_requires_review() {
    // This is already caught through the enclosing oneOf variant's member record;
    // it pins behavior but does not discriminate the multiset comparison change.
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["event"]["$defs"]["EventData"]["oneOf"][3]["properties"]["workspace"]
        ["anyOf"]
        .as_array_mut()
        .expect("anyOf is an array")
        .remove(1);
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("one-duplicate-removed", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_synthetic_root_duplicate_removal_requires_review() {
    let root = std::env::temp_dir().join(format!(
        "herdr-schema-review-{}-synthetic-root-duplicate",
        std::process::id()
    ));
    let script_dir = root.join("scripts");
    let fixture_dir = root.join("tests/fixtures/herdr-schema");
    std::fs::create_dir_all(&script_dir).expect("script dir");
    std::fs::create_dir_all(&fixture_dir).expect("fixture dir");

    let script = script_dir.join("review-herdr-protocol.sh");
    std::fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/review-herdr-protocol.sh"
        ),
        &script,
    )
    .expect("copy script");
    let baseline = serde_json::json!({
        "protocol": 20,
        "schemas": {
            "error_response": {
                "anyOf": [{"type": "null"}, {"type": "null"}]
            }
        }
    });
    std::fs::write(fixture_dir.join("baseline.json"), baseline.to_string())
        .expect("write synthetic baseline");
    let mut candidate = baseline;
    candidate["schemas"]["error_response"]["anyOf"]
        .as_array_mut()
        .expect("anyOf is an array")
        .remove(1);
    candidate["protocol"] = serde_json::json!(21);
    let candidate_path = root.join("candidate.json");
    std::fs::write(&candidate_path, candidate.to_string()).expect("write candidate");

    let out = Command::new("bash")
        .arg(&script)
        .args(["--candidate-file", candidate_path.to_str().expect("utf-8")])
        .output()
        .expect("copied script runs");
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_reordered_top_level_one_of_reviews_clean() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["event"]["$defs"]["EventData"]["oneOf"]
        .as_array_mut()
        .expect("oneOf is an array")
        .reverse();
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("top-level-one-of-reordered", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("verdict: additive or identical"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
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
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_plain_object_property_value_drift_requires_review() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["error_response"]["$defs"]["ErrorBody"]["properties"]["code"]["type"] =
        serde_json::json!("integer");
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("plain-object-property-value-drift", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_required_array_removal_requires_review() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["event"]["required"]
        .as_array_mut()
        .expect("required is an array")
        .remove(0);
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("required-removal", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_type_change_requires_review() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["title"] = serde_json::json!(12345);
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("type-change", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("verdict: REVIEW REQUIRED"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_purely_additive_newer_candidate_reviews_clean() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]
        .as_object_mut()
        .expect("schemas is an object")
        .insert(
            "brand_new_thing".to_owned(),
            serde_json::json!({"type": "object"}),
        );
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("additive", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("verdict: additive or identical"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("baseline protocol:"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_newer_candidate_with_appended_object_array_member_reviews_clean() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["schemas"]["event"]["$defs"]["EventData"]["oneOf"]
        .as_array_mut()
        .expect("oneOf is an array")
        .push(serde_json::json!({
            "type": "object",
            "properties": {
                "type": {
                    "const": "codex_test_event",
                    "type": "string"
                }
            },
            "required": ["type"]
        }));
    mutated["protocol"] = serde_json::json!(21);
    let candidate = temp_candidate("object-member-append", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("verdict: additive or identical"),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_non_integer_protocol_is_invalid_input() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    mutated["protocol"] = serde_json::json!(20.5);
    mutated["title"] = serde_json::json!("drifted");
    mutated["schema_version"] = serde_json::json!("drifted");
    let candidate = temp_candidate("non-integer-protocol", &mutated);
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn i7_protocol_arithmetic_injection_is_invalid_without_execution() {
    let baseline: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(BASELINE).expect("read"))
            .expect("baseline parses");
    let mut mutated = baseline.clone();
    let candidate = temp_candidate("arithmetic-injection", &mutated);
    let marker = candidate
        .parent()
        .expect("candidate directory")
        .join("PWNED");
    match std::fs::remove_file(&marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove stale marker: {error}"),
    }
    mutated["protocol"] = serde_json::Value::String(format!(
        "baseline[$(touch {})]",
        marker.to_str().expect("utf-8")
    ));
    std::fs::write(&candidate, mutated.to_string()).expect("rewrite candidate");
    let out = run(&["--candidate-file", candidate.to_str().expect("utf-8")]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !marker.exists(),
        "stdout: {} stderr: {} marker: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
        marker.display()
    );
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
