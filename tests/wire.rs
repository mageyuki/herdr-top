#[allow(dead_code)]
mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use herdr_top::herdr::types::{Snapshot, Subscription};
use herdr_top::herdr::wire::{WireError, request, subscribe};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[tokio::test]
async fn snapshot_parses_p1_fixture() {
    let response = fixture_payloads("p1-snapshot.jsonl", "A2", "recv")
        .pop()
        .expect("fixture should contain the A2 response");
    let config = MockConfig::default().respond("session.snapshot", response["result"].clone());
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");

    let result = request(mock.socket_path(), "session.snapshot", json!({}))
        .await
        .expect("snapshot request should succeed");
    assert_eq!(result.result_type(), "session_snapshot");
    let snapshot = result
        .into_snapshot()
        .expect("snapshot result should decode");

    assert_eq!(snapshot.version, "0.8.0");
    assert_eq!(snapshot.protocol, 19);
    assert_eq!(snapshot.workspaces.len(), 1);
    assert_eq!(snapshot.workspaces[0].workspace_id, "w1");
    assert_eq!(snapshot.tabs.len(), 1);
    assert_eq!(snapshot.tabs[0].tab_id, "w1:t1");
    assert_eq!(snapshot.panes.len(), 1);
    assert_eq!(snapshot.panes[0].pane_id, "w1:p1");
    assert_eq!(snapshot.panes[0].terminal_id, "term_6583d08d791e41");
}

#[tokio::test]
async fn push_envelope_is_event_data_only() {
    let received = fixture_payloads("p2-subscribe-push.jsonl", "B", "recv");
    let pushes: Vec<Value> = received
        .into_iter()
        .filter(|payload| payload.get("event").is_some())
        .collect();
    for push in &pushes {
        let keys: BTreeSet<&str> = push
            .as_object()
            .expect("push frame should be an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, BTreeSet::from(["data", "event"]));
    }

    let subscribe_request = fixture_payloads("p2-subscribe-push.jsonl", "B", "send")
        .pop()
        .expect("fixture should contain the B subscription request");
    let subscriptions: Vec<Subscription> =
        serde_json::from_value(subscribe_request["params"]["subscriptions"].clone())
            .expect("fixture subscriptions should decode");
    assert_eq!(
        serde_json::to_value(&subscriptions).expect("subscriptions should encode"),
        subscribe_request["params"]["subscriptions"]
    );

    let config = MockConfig::default().subscription_pushes(pushes.clone());
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");
    let mut events = subscribe(mock.socket_path(), &subscriptions)
        .await
        .expect("subscription should start");

    for expected in pushes {
        let (event, data) = events
            .next_event()
            .await
            .expect("push frame should decode")
            .expect("subscription should remain open");
        assert_eq!(event, expected["event"]);
        assert_eq!(data, expected["data"]);
        assert!(event.contains('_'));
    }
}

#[test]
fn unknown_fields_tolerated() {
    let mut snapshot = p1_snapshot_value();
    snapshot["future_snapshot_field"] = json!({"nested": true});
    snapshot["workspaces"][0]["future_workspace_field"] = json!(42);
    snapshot["panes"][0]["future_pane_field"] = json!(["new", "data"]);

    let decoded: Snapshot =
        serde_json::from_value(snapshot).expect("unknown fields should be ignored");
    assert_eq!(decoded.workspaces[0].workspace_id, "w1");
    assert_eq!(decoded.panes[0].pane_id, "w1:p1");
}

#[test]
fn missing_terminal_id_is_error_not_default() {
    let mut snapshot = p1_snapshot_value();
    snapshot["panes"][0]
        .as_object_mut()
        .expect("fixture pane should be an object")
        .remove("terminal_id");

    let decoded = serde_json::from_value::<Snapshot>(snapshot);
    assert!(decoded.is_err(), "terminal_id must not default to empty");
}

#[tokio::test]
async fn error_envelope_surfaces_typed() {
    // The error envelope is schema-declared, not transcript-observed; the mock fabricates it.
    let config = MockConfig::default().error("schema.failure", "E_SCHEMA", "fabricated failure");
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");

    let error = request(mock.socket_path(), "schema.failure", json!({}))
        .await
        .expect_err("error envelope should fail the request");
    match error {
        WireError::Server { code, message } => {
            assert_eq!(code, "E_SCHEMA");
            assert_eq!(message, "fabricated failure");
        }
        other => panic!("expected typed server error, got {other:?}"),
    }
}

#[tokio::test]
async fn one_request_per_connection_close_honored() {
    let response = fixture_payloads("p0-conn-semantics.jsonl", "T1", "recv")
        .pop()
        .expect("fixture should contain the T1 response");
    let config = MockConfig::default().respond("ping", response["result"].clone());
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");

    let stream = UnixStream::connect(mock.socket_path())
        .await
        .expect("raw client should connect");
    let mut reader = BufReader::new(stream);
    reader
        .get_mut()
        .write_all(b"{\"id\":\"raw-1\",\"method\":\"ping\",\"params\":{}}\n")
        .await
        .expect("first raw request should write");
    let mut line = String::new();
    assert_ne!(
        reader
            .read_line(&mut line)
            .await
            .expect("first raw response should read"),
        0
    );

    let second_write = reader
        .get_mut()
        .write_all(b"{\"id\":\"raw-2\",\"method\":\"ping\",\"params\":{}}\n")
        .await;
    if second_write.is_ok() {
        line.clear();
        match tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .expect("closed connection should not hang")
        {
            Ok(0) | Err(_) => {}
            Ok(_) => panic!("a second response must not be sent"),
        }
    }

    let connections_before_client = mock.accepted_connections();
    let first = request(mock.socket_path(), "ping", json!({}))
        .await
        .expect("first client request should succeed");
    let second = request(mock.socket_path(), "ping", json!({}))
        .await
        .expect("second client request should succeed");
    assert_eq!(first.result_type(), "pong");
    assert_eq!(second.result_type(), "pong");
    assert_eq!(
        mock.accepted_connections(),
        connections_before_client + 2,
        "each client request must open a distinct connection"
    );

    let requests = mock.requests();
    let client_requests = &requests[requests.len() - 2..];
    assert_ne!(client_requests[0]["id"], client_requests[1]["id"]);
    assert!(
        client_requests
            .iter()
            .all(|request| request["params"] == json!({}))
    );
}

fn p1_snapshot_value() -> Value {
    fixture_payloads("p1-snapshot.jsonl", "A2", "recv")
        .pop()
        .expect("fixture should contain the A2 response")["result"]["snapshot"]
        .clone()
}
