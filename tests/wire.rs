#[allow(dead_code)]
mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use herdr_top::herdr::types::{AgentManifestStatus, Pong, Snapshot, Subscription};
use herdr_top::herdr::wire::{WireError, agent_manifests, ping, request, subscribe};
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

#[tokio::test]
async fn i4_remote_unknown_wire_fields_are_tolerated() {
    let pong_result = json!({
        "type": "pong",
        "version": "0.8.0",
        "protocol": 19,
        "capabilities": {
            "live_handoff": true,
            "future_capability": {"nested": true}
        },
        "future_pong_field": [1, 2, 3]
    });
    let manifest_result = json!({
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
            "warning": null,
            "future_manifest_field": {"nested": true}
        }],
        "last_check_unix": 1,
        "last_result": "current",
        "future_status_field": true
    });
    let config = MockConfig::default()
        .respond("ping", pong_result)
        .respond("server.agent_manifests", manifest_result);
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");

    let pong = ping(mock.socket_path()).await.expect("pong should decode");
    assert_eq!(pong.version, "0.8.0");
    assert_eq!(pong.protocol, 19);
    let capabilities = pong.capabilities.expect("capabilities should decode");
    assert!(capabilities.live_handoff);
    assert!(!capabilities.detached_server_daemon);

    let status = agent_manifests(mock.socket_path())
        .await
        .expect("manifest status should decode");
    assert_eq!(status.manifests.len(), 1);
    assert_eq!(status.manifests[0].agent, "claude");
    assert_eq!(status.manifests[0].active_version.as_deref(), Some("6"));

    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "ping");
    assert_eq!(requests[0]["params"], json!({}));
    assert_eq!(requests[1]["method"], "server.agent_manifests");
    assert_eq!(requests[1]["params"], json!({}));

    for invalid in [
        json!({"type": "pong", "protocol": 19}),
        json!({"type": "pong", "version": null, "protocol": 19}),
        json!({"type": "pong", "version": 8, "protocol": 19}),
        json!({"type": "pong", "version": "0.8.0", "protocol": -1}),
        json!({"type": "pong", "version": "0.8.0", "protocol": 19, "capabilities": {}}),
    ] {
        assert!(serde_json::from_value::<Pong>(invalid).is_err());
    }
    for invalid in [
        json!({"manifests": []}),
        json!({"type": "agent_manifest_status"}),
        json!({"type": "agent_manifest_status", "manifests": null}),
        json!({"type": "agent_manifest_status", "manifests": [{
            "source": "x", "source_kind": "remote", "local_override_shadowing_remote": false
        }]}),
    ] {
        assert!(serde_json::from_value::<AgentManifestStatus>(invalid).is_err());
    }
}

#[tokio::test]
async fn i4_remote_wrong_result_type_fails_closed() {
    let config = MockConfig::default()
        .respond(
            "ping",
            json!({"type": "agent_manifest_status", "version": "0.8.0", "protocol": 19}),
        )
        .respond(
            "server.agent_manifests",
            json!({"type": "pong", "manifests": []}),
        );
    let mock = MockHerdr::start(config)
        .await
        .expect("mock server should bind");

    assert!(matches!(
        ping(mock.socket_path()).await,
        Err(WireError::UnexpectedResponse(_))
    ));
    assert!(matches!(
        agent_manifests(mock.socket_path()).await,
        Err(WireError::UnexpectedResponse(_))
    ));

    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "ping");
    assert_eq!(requests[0]["params"], json!({}));
    assert_eq!(requests[1]["method"], "server.agent_manifests");
    assert_eq!(requests[1]["params"], json!({}));
}

#[test]
fn i4_remote_pong_protocol_uint32_boundary_fails_closed() {
    assert!(
        serde_json::from_value::<Pong>(json!({
            "type": "pong",
            "version": "0.8.0"
        }))
        .is_err(),
        "protocol must remain required"
    );

    let maximum = serde_json::from_value::<Pong>(json!({
        "type": "pong",
        "version": "0.8.0",
        "protocol": u32::MAX
    }))
    .expect("the schema-declared uint32 maximum should decode");
    assert_eq!(u64::from(maximum.protocol), u64::from(u32::MAX));

    let overflow =
        serde_json::from_str::<Pong>(r#"{"type":"pong","version":"0.8.0","protocol":4294967296}"#);
    assert!(
        overflow.is_err(),
        "a protocol above the schema uint32 range must fail typed decoding"
    );
}

fn p1_snapshot_value() -> Value {
    fixture_payloads("p1-snapshot.jsonl", "A2", "recv")
        .pop()
        .expect("fixture should contain the A2 response")["result"]["snapshot"]
        .clone()
}
