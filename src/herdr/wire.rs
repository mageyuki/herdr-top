//! Newline-delimited JSON client for the herdr Unix socket.

use std::io;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const IO_TIMEOUT: Duration = Duration::from_secs(5);

use super::types::{AgentManifestStatus, ErrorBody, Pong, Snapshot, Subscription};

/// Errors produced while exchanging herdr wire frames.
#[derive(Debug, Error)]
pub enum WireError {
    #[error("herdr wire I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("malformed herdr wire frame: {0}")]
    MalformedFrame(String),
    #[error("herdr server error {code}: {message}")]
    Server { code: String, message: String },
    #[error("unexpected herdr response: {0}")]
    UnexpectedResponse(String),
    #[error("herdr wire {operation} timed out after {seconds} seconds")]
    Timeout {
        operation: &'static str,
        seconds: u64,
    },
}

/// A successful result object returned by herdr.
#[derive(Clone, Debug)]
pub struct WireResult {
    result_type: String,
    payload: Value,
}

impl WireResult {
    /// Returns the result object's `type` tag.
    pub fn result_type(&self) -> &str {
        &self.result_type
    }

    /// Returns the complete result payload, including its `type` tag.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Decodes the complete result payload into a caller-selected type.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, WireError> {
        serde_json::from_value(self.payload.clone()).map_err(malformed_json)
    }

    /// Extracts and decodes the snapshot from a `session_snapshot` result.
    pub fn into_snapshot(self) -> Result<Snapshot, WireError> {
        if self.result_type != "session_snapshot" {
            return Err(WireError::UnexpectedResponse(format!(
                "expected session_snapshot, received {}",
                self.result_type
            )));
        }
        let snapshot =
            self.payload.get("snapshot").cloned().ok_or_else(|| {
                WireError::MalformedFrame("snapshot result has no snapshot".into())
            })?;
        serde_json::from_value(snapshot).map_err(malformed_json)
    }

    fn from_payload(payload: Value) -> Result<Self, WireError> {
        let result_type = payload
            .as_object()
            .and_then(|object| object.get("type"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WireError::MalformedFrame("result must be an object with a string type".into())
            })?
            .to_owned();
        Ok(Self {
            result_type,
            payload,
        })
    }
}

/// Dedicated event subscription connection.
///
/// It intentionally exposes only event reads: requests use fresh connections.
pub struct EventStream {
    reader: BufReader<UnixStream>,
}

impl EventStream {
    /// Reads the next pushed event, or returns `None` after a clean EOF.
    pub async fn next_event(&mut self) -> Result<Option<(String, Value)>, WireError> {
        let Some(frame) = read_value(&mut self.reader).await? else {
            return Ok(None);
        };
        let Value::Object(mut object) = frame else {
            return Err(WireError::MalformedFrame(
                "event push must be a JSON object".into(),
            ));
        };
        if object.len() != 2 || !object.contains_key("event") || !object.contains_key("data") {
            return Err(WireError::MalformedFrame(
                "event push must contain exactly event and data".into(),
            ));
        }
        let event = object
            .remove("event")
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| WireError::MalformedFrame("event must be a string".into()))?;
        let data = object
            .remove("data")
            .ok_or_else(|| WireError::MalformedFrame("event push has no data".into()))?;
        Ok(Some((event, data)))
    }

    pub(crate) async fn close(mut self) -> Result<(), WireError> {
        timeout("subscription close", self.reader.get_mut().shutdown()).await??;
        Ok(())
    }
}

/// Sends one request over a fresh Unix-socket connection.
pub async fn request(sock: &Path, method: &str, params: Value) -> Result<WireResult, WireError> {
    let mut stream = timeout("connect", UnixStream::connect(sock)).await??;
    let id = ulid::Ulid::new().to_string();
    timeout("request", async move {
        write_request(&mut stream, &id, method, params).await?;
        let mut reader = BufReader::new(stream);
        read_response(&mut reader, &id).await
    })
    .await?
}

/// Queries and decodes the schema-declared Herdr socket handshake.
pub async fn ping(sock: &Path) -> Result<Pong, WireError> {
    decode_typed_result(request(sock, "ping", json!({})).await?, "pong")
}

/// Queries and decodes the installed official provider manifests.
pub async fn agent_manifests(sock: &Path) -> Result<AgentManifestStatus, WireError> {
    decode_typed_result(
        request(sock, "server.agent_manifests", json!({})).await?,
        "agent_manifest_status",
    )
}

fn decode_typed_result<T: DeserializeOwned>(
    result: WireResult,
    expected_type: &str,
) -> Result<T, WireError> {
    if result.result_type() != expected_type {
        return Err(WireError::UnexpectedResponse(format!(
            "expected {expected_type}, received {}",
            result.result_type()
        )));
    }
    result.decode()
}

/// Opens an event-only connection and validates its subscription acknowledgement.
pub async fn subscribe(sock: &Path, subs: &[Subscription]) -> Result<EventStream, WireError> {
    let mut stream = timeout("connect", UnixStream::connect(sock)).await??;
    let id = ulid::Ulid::new().to_string();
    timeout("subscription acknowledgement", async move {
        write_request(
            &mut stream,
            &id,
            "events.subscribe",
            json!({"subscriptions": subs}),
        )
        .await?;

        let mut reader = BufReader::new(stream);
        let acknowledgement = read_response(&mut reader, &id).await?;
        if acknowledgement.result_type() != "subscription_started" {
            return Err(WireError::UnexpectedResponse(format!(
                "expected subscription_started, received {}",
                acknowledgement.result_type()
            )));
        }
        Ok(EventStream { reader })
    })
    .await?
}

async fn timeout<T>(
    operation: &'static str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, WireError> {
    tokio::time::timeout(IO_TIMEOUT, future)
        .await
        .map_err(|_| WireError::Timeout {
            operation,
            seconds: IO_TIMEOUT.as_secs(),
        })
}

#[derive(Deserialize)]
struct ResponseEnvelope {
    id: String,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ErrorBody>,
}

async fn write_request(
    stream: &mut UnixStream,
    id: &str,
    method: &str,
    params: Value,
) -> Result<(), WireError> {
    let params = if params.is_null() { json!({}) } else { params };
    if !params.is_object() {
        return Err(WireError::MalformedFrame(
            "request params must be a JSON object".into(),
        ));
    }
    let frame = json!({"id": id, "method": method, "params": params});
    let mut bytes = serde_json::to_vec(&frame).map_err(malformed_json)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_response(
    reader: &mut BufReader<UnixStream>,
    expected_id: &str,
) -> Result<WireResult, WireError> {
    let frame = read_value(reader)
        .await?
        .ok_or_else(|| WireError::MalformedFrame("EOF before response".into()))?;
    let envelope: ResponseEnvelope = serde_json::from_value(frame).map_err(malformed_json)?;
    let matching_decorated_error = envelope.result.is_none()
        && envelope.error.is_some()
        && envelope
            .id
            .strip_prefix(expected_id)
            .is_some_and(|suffix| suffix.starts_with(':'));
    if envelope.id != expected_id && !matching_decorated_error {
        return Err(WireError::UnexpectedResponse(format!(
            "expected response id {expected_id}, received {}",
            envelope.id
        )));
    }
    match (envelope.result, envelope.error) {
        (Some(result), None) => WireResult::from_payload(result),
        (None, Some(ErrorBody { code, message })) => Err(WireError::Server { code, message }),
        (Some(_), Some(_)) => Err(WireError::MalformedFrame(
            "response contains both result and error".into(),
        )),
        (None, None) => Err(WireError::MalformedFrame(
            "response contains neither result nor error".into(),
        )),
    }
}

async fn read_value(reader: &mut BufReader<UnixStream>) -> Result<Option<Value>, WireError> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    serde_json::from_str(&line)
        .map(Some)
        .map_err(malformed_json)
}

fn malformed_json(error: serde_json::Error) -> WireError {
    WireError::MalformedFrame(error.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn decode_response(frame: Value, expected_id: &str) -> Result<WireResult, WireError> {
        let (mut server, client) = UnixStream::pair().expect("Unix stream pair should open");
        let mut bytes = serde_json::to_vec(&frame).expect("response should encode");
        bytes.push(b'\n');
        server
            .write_all(&bytes)
            .await
            .expect("response should write");
        drop(server);
        read_response(&mut BufReader::new(client), expected_id).await
    }

    #[tokio::test]
    async fn decorated_ids_are_accepted_only_for_matching_error_responses() {
        let error = decode_response(
            json!({
                "id": "req:sub:1:probe",
                "error": {"code": "pane_not_found", "message": "pane w9:p99 not found"}
            }),
            "req",
        )
        .await
        .expect_err("matching decorated error should surface as a server error");
        assert!(matches!(
            error,
            WireError::Server { ref code, ref message }
                if code == "pane_not_found" && message.contains("w9:p99")
        ));

        let unrelated = decode_response(
            json!({
                "id": "other:sub:1:probe",
                "error": {"code": "pane_not_found", "message": "pane w9:p99 not found"}
            }),
            "req",
        )
        .await
        .expect_err("an unrelated decorated id must be rejected");
        assert!(matches!(unrelated, WireError::UnexpectedResponse(_)));

        let decorated_success = decode_response(
            json!({
                "id": "req:sub:1:probe",
                "result": {"type": "subscription_started"}
            }),
            "req",
        )
        .await
        .expect_err("successful responses must echo the request id verbatim");
        assert!(matches!(
            decorated_success,
            WireError::UnexpectedResponse(_)
        ));
    }
}
