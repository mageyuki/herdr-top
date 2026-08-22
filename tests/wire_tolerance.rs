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
    for payload in payloads {
        let mut snapshot = payload["snapshot"].clone();
        assert!(snapshot.is_object(), "snapshot payload is an object");
        inject_unknown_fields(&mut snapshot);
        serde_json::from_value::<Snapshot>(snapshot)
            .expect("snapshot deserializes with unknown fields injected");
    }
}
