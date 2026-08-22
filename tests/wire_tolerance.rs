//! Pins the additive tolerance the newer-protocol Warning tier relies on:
//! every typed inbound wire payload must deserialize when unknown fields
//! appear at any object level. A `deny_unknown_fields` regression fails these
//! tests.

use herdr_top::herdr::types::{AgentManifestInfo, AgentManifestStatus, Pong, Snapshot};
use serde_json::{Value, json};

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

fn recv_results(fixture: &str, kind: &str) -> Vec<Value> {
    let path = format!(
        "{}/tests/fixtures/wire/{fixture}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(path).expect("wire fixture is readable");
    raw.lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("fixture line is valid JSON"))
        .filter(|entry| entry["dir"] == "recv")
        .filter_map(|entry| {
            let result = entry["payload"]["result"].clone();
            (result["type"] == kind).then_some(result)
        })
        .collect()
}

fn agent_manifest_status_payload() -> Value {
    json!({
        "type": "agent_manifest_status",
        "manifests": [{
            "agent": "claude",
            "source": "/private/manifest-path",
            "source_kind": "remote",
            "local_override_shadowing_remote": false,
            "active_version": "6",
            "cached_remote_version": null,
            "remote_last_checked_unix": 1,
            "remote_update_error": null,
            "remote_update_result": "current",
            "warning": null
        }],
        "last_check_unix": 1,
        "last_result": "current"
    })
}

#[test]
fn i7_pong_tolerates_unknown_fields_at_every_level() {
    let payloads = recv_results("p1-snapshot.jsonl", "pong");
    assert!(!payloads.is_empty(), "fixture must contain pong results");
    for mut payload in payloads {
        inject_unknown_fields(&mut payload);
        serde_json::from_value::<Pong>(payload)
            .expect("pong deserializes with unknown fields injected");
    }
}

#[test]
fn i7_snapshot_tolerates_unknown_fields_at_every_level() {
    let payloads = recv_results("p1-snapshot.jsonl", "session_snapshot");
    assert!(
        !payloads.is_empty(),
        "fixture must contain session_snapshot results"
    );
    for payload in payloads {
        let mut snapshot = payload["snapshot"].clone();
        assert!(snapshot.is_object(), "snapshot payload is an object");
        inject_unknown_fields(&mut snapshot);
        serde_json::from_value::<Snapshot>(snapshot)
            .expect("snapshot deserializes with unknown fields injected");
    }
}

#[test]
fn i7_agent_session_info_tolerates_unknown_fields_at_every_level() {
    let payloads = recv_results("p6-cold-restart.jsonl", "session_snapshot");
    assert!(
        !payloads.is_empty(),
        "fixture must contain session_snapshot results"
    );
    let mut agent_session_count = 0;
    for payload in payloads {
        let mut snapshot = payload["snapshot"].clone();
        assert!(snapshot.is_object(), "snapshot payload is an object");
        inject_unknown_fields(&mut snapshot);
        let snapshot = serde_json::from_value::<Snapshot>(snapshot)
            .expect("snapshot with agent sessions tolerates unknown fields");
        agent_session_count += snapshot
            .panes
            .iter()
            .filter(|pane| pane.agent_session.is_some())
            .count();
    }
    assert!(
        agent_session_count > 0,
        "decoded fixture must contain at least one pane with an agent_session"
    );
}

#[test]
fn i7_agent_manifest_status_tolerates_unknown_fields_at_every_level() {
    let mut payload = agent_manifest_status_payload();
    assert!(
        payload["manifests"]
            .as_array()
            .is_some_and(|manifests| !manifests.is_empty()),
        "manifest payload must contain at least one manifest"
    );
    inject_unknown_fields(&mut payload);

    let status = serde_json::from_value::<AgentManifestStatus>(payload)
        .expect("manifest status tolerates unknown fields");
    assert!(
        !status.manifests.is_empty(),
        "decoded manifest status must contain at least one manifest"
    );
}

#[test]
fn i7_agent_manifest_info_tolerates_unknown_fields_at_every_level() {
    let payload = agent_manifest_status_payload();
    let manifests = payload["manifests"]
        .as_array()
        .expect("manifest payload contains a manifests array");
    assert!(
        !manifests.is_empty(),
        "manifest payload must contain at least one manifest"
    );

    for manifest in manifests {
        let mut manifest = manifest.clone();
        inject_unknown_fields(&mut manifest);
        serde_json::from_value::<AgentManifestInfo>(manifest)
            .expect("manifest info tolerates unknown fields");
    }
}
