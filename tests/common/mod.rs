//! Shared integration-test helpers.

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// Loads one flat provider JSONL fixture as `(byte_offset, record_bytes)` pairs.
#[allow(dead_code)]
pub fn flat_jsonl_fixture(file_name: &str) -> Vec<(u64, Vec<u8>)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/provider")
        .join(file_name);
    let bytes = fs::read(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let mut offset = 0_u64;

    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|record| {
            let record_offset = offset;
            offset = offset.saturating_add(record.len() as u64);
            let mut record = record.strip_suffix(b"\n").unwrap_or(record);
            record = record.strip_suffix(b"\r").unwrap_or(record);
            (record_offset, record.to_vec())
        })
        .collect()
}

/// Re-executes this integration-test binary and runs one exact helper test.
///
/// Re-exec avoids `fork` in libtest's threaded process and lets tests exercise
/// behavior that must cross an OS process boundary.
#[allow(dead_code)]
pub fn spawn_self_test_helper(test_name: &str, envs: &[(&str, &OsStr)]) -> Output {
    Command::new(std::env::current_exe().expect("current test executable should be available"))
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .envs(envs.iter().copied())
        .output()
        .expect("helper test process should start")
}

// This module is shared by integration-test binaries that do not all use the mock.
#[allow(dead_code)]
pub mod mock {
    use std::collections::HashMap;
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug, Default)]
    pub struct MockConfig {
        responses: HashMap<String, Value>,
        errors: HashMap<String, (String, String)>,
        subscription_pushes: Vec<Value>,
    }

    impl MockConfig {
        pub fn respond(mut self, method: &str, result: Value) -> Self {
            self.responses.insert(method.to_owned(), result);
            self
        }

        pub fn error(mut self, method: &str, code: &str, message: &str) -> Self {
            self.errors
                .insert(method.to_owned(), (code.to_owned(), message.to_owned()));
            self
        }

        pub fn subscription_pushes(mut self, pushes: Vec<Value>) -> Self {
            self.subscription_pushes = pushes;
            self
        }
    }

    pub struct MockHerdr {
        _temp_dir: TempDir,
        socket_path: PathBuf,
        accepted_connections: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Value>>>,
        accept_task: JoinHandle<()>,
    }

    impl MockHerdr {
        pub async fn start(config: MockConfig) -> io::Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let socket_path = temp_dir.path().join("herdr.sock");
            let listener = UnixListener::bind(&socket_path)?;
            let accepted_connections = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let task_connections = Arc::clone(&accepted_connections);
            let task_requests = Arc::clone(&requests);
            let config = Arc::new(config);

            let accept_task = tokio::spawn(async move {
                while let Ok((stream, _address)) = listener.accept().await {
                    task_connections.fetch_add(1, Ordering::SeqCst);
                    let config = Arc::clone(&config);
                    let requests = Arc::clone(&task_requests);
                    tokio::spawn(async move {
                        let _ = handle_connection(stream, &config, &requests).await;
                    });
                }
            });

            Ok(Self {
                _temp_dir: temp_dir,
                socket_path,
                accepted_connections,
                requests,
                accept_task,
            })
        }

        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        pub fn accepted_connections(&self) -> usize {
            self.accepted_connections.load(Ordering::SeqCst)
        }

        pub fn requests(&self) -> Vec<Value> {
            self.requests
                .lock()
                .expect("mock request log mutex should not be poisoned")
                .clone()
        }
    }

    impl Drop for MockHerdr {
        fn drop(&mut self) {
            self.accept_task.abort();
        }
    }

    pub fn fixture_payloads(file_name: &str, connection: &str, direction: &str) -> Vec<Value> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wire")
            .join(file_name);
        let transcript = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

        transcript
            .lines()
            .enumerate()
            .filter_map(|(line_number, line)| {
                let record: Value = serde_json::from_str(line).unwrap_or_else(|error| {
                    panic!(
                        "failed to parse {} line {}: {error}",
                        path.display(),
                        line_number + 1
                    )
                });
                (record.get("conn").and_then(Value::as_str) == Some(connection)
                    && record.get("dir").and_then(Value::as_str) == Some(direction))
                .then(|| record["payload"].clone())
            })
            .collect()
    }

    async fn handle_connection(
        stream: UnixStream,
        config: &MockConfig,
        requests: &Mutex<Vec<Value>>,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let request: Value = serde_json::from_str(&line).map_err(invalid_data)?;
        requests
            .lock()
            .map_err(|_| io::Error::other("mock request log mutex was poisoned"))?
            .push(request.clone());

        let id = required_string(&request, "id")?;
        let method = required_string(&request, "method")?;
        if method == "events.subscribe" {
            write_frame(
                reader.get_mut(),
                &json!({"id": id, "result": {"type": "subscription_started"}}),
            )
            .await?;
            for push in &config.subscription_pushes {
                write_frame(reader.get_mut(), push).await?;
            }

            line.clear();
            let _ = reader.read_line(&mut line).await;
            return Ok(());
        }

        let response = if let Some((code, message)) = config.errors.get(method) {
            json!({"id": id, "error": {"code": code, "message": message}})
        } else if let Some(result) = config.responses.get(method) {
            json!({"id": id, "result": result})
        } else {
            json!({
                "id": id,
                "error": {"code": "METHOD_NOT_FOUND", "message": "no canned response"}
            })
        };
        write_frame(reader.get_mut(), &response).await
    }

    async fn write_frame(stream: &mut UnixStream, frame: &Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(frame).map_err(invalid_data)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.flush().await
    }

    fn required_string<'a>(value: &'a Value, key: &str) -> io::Result<&'a str> {
        value.get(key).and_then(Value::as_str).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("request is missing string field {key}"),
            )
        })
    }

    fn invalid_data(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
    }
}

// This is intentionally a separate, append-only helper: the T8 mock above keeps
// its one-response-per-method behavior unchanged for the existing wire tests.
#[allow(dead_code)]
pub mod scripted_mock {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug, Default)]
    pub struct ScriptedConfig {
        snapshots: Vec<Value>,
        generations: Vec<Vec<Value>>,
        close_after_snapshots: Vec<usize>,
        snapshot_delay: Duration,
    }

    impl ScriptedConfig {
        pub fn snapshots(mut self, snapshots: Vec<Value>) -> Self {
            self.snapshots = snapshots;
            self
        }

        pub fn generations(mut self, generations: Vec<Vec<Value>>) -> Self {
            self.generations = generations;
            self
        }

        pub fn close_after_snapshots(mut self, indexes: Vec<usize>) -> Self {
            self.close_after_snapshots = indexes;
            self
        }

        pub fn snapshot_delay(mut self, delay: Duration) -> Self {
            self.snapshot_delay = delay;
            self
        }
    }

    pub struct ScriptedHerdr {
        _temp_dir: TempDir,
        socket_path: PathBuf,
        accepted_connections: Arc<AtomicUsize>,
        subscription_connections: Arc<AtomicUsize>,
        snapshot_requests: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Value>>>,
        config: Arc<ScriptedConfig>,
        active_subscription: Arc<Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>>,
        accept_task: JoinHandle<()>,
    }

    impl ScriptedHerdr {
        pub async fn start(config: ScriptedConfig) -> io::Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let socket_path = temp_dir.path().join("herdr.sock");
            let listener = UnixListener::bind(&socket_path)?;
            let accepted_connections = Arc::new(AtomicUsize::new(0));
            let subscription_connections = Arc::new(AtomicUsize::new(0));
            let snapshot_requests = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let active_subscription = Arc::new(Mutex::new(None));
            let config = Arc::new(config);
            let accept_task = spawn_accept_task(
                listener,
                Arc::clone(&config),
                Arc::clone(&accepted_connections),
                Arc::clone(&subscription_connections),
                Arc::clone(&snapshot_requests),
                Arc::clone(&requests),
                Arc::clone(&active_subscription),
            );

            Ok(Self {
                _temp_dir: temp_dir,
                socket_path,
                accepted_connections,
                subscription_connections,
                snapshot_requests,
                requests,
                config,
                active_subscription,
                accept_task,
            })
        }

        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        pub fn accepted_connections(&self) -> usize {
            self.accepted_connections.load(Ordering::SeqCst)
        }

        pub fn subscription_connections(&self) -> usize {
            self.subscription_connections.load(Ordering::SeqCst)
        }

        pub fn snapshot_requests(&self) -> usize {
            self.snapshot_requests.load(Ordering::SeqCst)
        }

        pub fn requests(&self) -> Vec<Value> {
            self.requests
                .lock()
                .expect("scripted mock request log should not be poisoned")
                .clone()
        }

        pub async fn replace_socket(&mut self) -> io::Result<()> {
            let active = self
                .active_subscription
                .lock()
                .map_err(|_| io::Error::other("active subscription mutex was poisoned"))?
                .clone();
            if let Some(active) = active {
                let _ = active.send(SubscriptionCommand::Close);
            }
            self.accept_task.abort();
            match std::fs::remove_file(&self.socket_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            let listener = UnixListener::bind(&self.socket_path)?;
            self.accept_task = spawn_accept_task(
                listener,
                Arc::clone(&self.config),
                Arc::clone(&self.accepted_connections),
                Arc::clone(&self.subscription_connections),
                Arc::clone(&self.snapshot_requests),
                Arc::clone(&self.requests),
                Arc::clone(&self.active_subscription),
            );
            Ok(())
        }
    }

    impl Drop for ScriptedHerdr {
        fn drop(&mut self) {
            self.accept_task.abort();
        }
    }

    enum SubscriptionCommand {
        Push(Vec<Value>, oneshot::Sender<()>),
        Close,
    }

    fn spawn_accept_task(
        listener: UnixListener,
        config: Arc<ScriptedConfig>,
        accepted_connections: Arc<AtomicUsize>,
        subscription_connections: Arc<AtomicUsize>,
        snapshot_requests: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Value>>>,
        active_subscription: Arc<Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted_connections.fetch_add(1, Ordering::SeqCst);
                let config = Arc::clone(&config);
                let subscriptions = Arc::clone(&subscription_connections);
                let snapshots = Arc::clone(&snapshot_requests);
                let requests = Arc::clone(&requests);
                let active = Arc::clone(&active_subscription);
                tokio::spawn(async move {
                    let _ = handle_connection(
                        stream,
                        &config,
                        &subscriptions,
                        &snapshots,
                        &requests,
                        &active,
                    )
                    .await;
                });
            }
        })
    }

    async fn handle_connection(
        stream: UnixStream,
        config: &ScriptedConfig,
        subscription_connections: &AtomicUsize,
        snapshot_requests: &AtomicUsize,
        requests: &Mutex<Vec<Value>>,
        active_subscription: &Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let request: Value = serde_json::from_str(&line).map_err(invalid_data)?;
        requests
            .lock()
            .map_err(|_| io::Error::other("scripted request log was poisoned"))?
            .push(request.clone());
        let id = required_string(&request, "id")?.to_owned();
        let method = required_string(&request, "method")?;

        if method == "events.subscribe" {
            subscription_connections.fetch_add(1, Ordering::SeqCst);
            let (sender, mut commands) = mpsc::unbounded_channel();
            *active_subscription
                .lock()
                .map_err(|_| io::Error::other("active subscription mutex was poisoned"))? =
                Some(sender);
            write_frame(
                reader.get_mut(),
                &json!({"id": id, "result": {"type": "subscription_started"}}),
            )
            .await?;
            while let Some(command) = commands.recv().await {
                match command {
                    SubscriptionCommand::Push(frames, acknowledgement) => {
                        for frame in frames {
                            write_frame(reader.get_mut(), &frame).await?;
                        }
                        let _ = acknowledgement.send(());
                    }
                    SubscriptionCommand::Close => return Ok(()),
                }
            }
            return Ok(());
        }

        if method == "session.snapshot" {
            let index = snapshot_requests.fetch_add(1, Ordering::SeqCst);
            let sender = active_subscription
                .lock()
                .map_err(|_| io::Error::other("active subscription mutex was poisoned"))?
                .clone();
            if let Some(sender) = sender {
                let frames = config.generations.get(index).cloned().unwrap_or_default();
                let (acknowledgement, response) = oneshot::channel();
                if sender
                    .send(SubscriptionCommand::Push(frames, acknowledgement))
                    .is_ok()
                {
                    let _ = response.await;
                }
            }
            if !config.snapshot_delay.is_zero() {
                tokio::time::sleep(config.snapshot_delay).await;
            } else {
                tokio::task::yield_now().await;
            }
            let snapshot = config
                .snapshots
                .get(index)
                .or_else(|| config.snapshots.last())
                .ok_or_else(|| io::Error::other("scripted mock has no snapshot"))?;
            write_frame(
                reader.get_mut(),
                &json!({"id": id, "result": {"type": "session_snapshot", "snapshot": snapshot}}),
            )
            .await?;
            if config.close_after_snapshots.contains(&index) {
                let sender = active_subscription
                    .lock()
                    .map_err(|_| io::Error::other("active subscription mutex was poisoned"))?
                    .clone();
                if let Some(sender) = sender {
                    let _ = sender.send(SubscriptionCommand::Close);
                }
            }
            return Ok(());
        }

        write_frame(
            reader.get_mut(),
            &json!({"id": id, "error": {"code": "METHOD_NOT_FOUND", "message": "no scripted response"}}),
        )
        .await
    }

    async fn write_frame(stream: &mut UnixStream, frame: &Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(frame).map_err(invalid_data)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.flush().await
    }

    fn required_string<'a>(value: &'a Value, key: &str) -> io::Result<&'a str> {
        value.get(key).and_then(Value::as_str).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("request is missing string field {key}"),
            )
        })
    }

    fn invalid_data(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
    }
}

// Hardening-only mock with ordered snapshot failures and subscription-lifecycle counters.
// Appended separately so the earlier shared mocks remain byte-for-byte behavior compatible.
#[allow(dead_code)]
pub mod hardening_mock {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug)]
    pub enum SnapshotReply {
        Snapshot(Value),
        Error,
    }

    #[derive(Clone, Debug, Default)]
    pub struct HardeningConfig {
        replies: Vec<SnapshotReply>,
        hang_subscription_ack: bool,
    }

    impl HardeningConfig {
        pub fn replies(mut self, replies: Vec<SnapshotReply>) -> Self {
            self.replies = replies;
            self
        }

        pub fn hang_subscription_ack(mut self) -> Self {
            self.hang_subscription_ack = true;
            self
        }
    }

    pub struct HardeningHerdr {
        _temp_dir: TempDir,
        socket_path: PathBuf,
        subscriptions: Arc<AtomicUsize>,
        active_subscriptions: Arc<AtomicUsize>,
        joined_subscriptions: Arc<AtomicUsize>,
        snapshot_requests: Arc<AtomicUsize>,
        accept_task: JoinHandle<()>,
    }

    impl HardeningHerdr {
        pub async fn start(config: HardeningConfig) -> io::Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let socket_path = temp_dir.path().join("herdr.sock");
            let listener = UnixListener::bind(&socket_path)?;
            let subscriptions = Arc::new(AtomicUsize::new(0));
            let active_subscriptions = Arc::new(AtomicUsize::new(0));
            let joined_subscriptions = Arc::new(AtomicUsize::new(0));
            let snapshot_requests = Arc::new(AtomicUsize::new(0));
            let config = Arc::new(config);
            let task_subscriptions = Arc::clone(&subscriptions);
            let task_active = Arc::clone(&active_subscriptions);
            let task_joined = Arc::clone(&joined_subscriptions);
            let task_snapshots = Arc::clone(&snapshot_requests);
            let accept_task = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let config = Arc::clone(&config);
                    let subscriptions = Arc::clone(&task_subscriptions);
                    let active = Arc::clone(&task_active);
                    let joined = Arc::clone(&task_joined);
                    let snapshots = Arc::clone(&task_snapshots);
                    tokio::spawn(async move {
                        let _ = handle_connection(
                            stream,
                            &config,
                            &subscriptions,
                            &active,
                            &joined,
                            &snapshots,
                        )
                        .await;
                    });
                }
            });
            Ok(Self {
                _temp_dir: temp_dir,
                socket_path,
                subscriptions,
                active_subscriptions,
                joined_subscriptions,
                snapshot_requests,
                accept_task,
            })
        }

        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        pub fn subscriptions(&self) -> usize {
            self.subscriptions.load(Ordering::SeqCst)
        }

        pub fn active_subscriptions(&self) -> usize {
            self.active_subscriptions.load(Ordering::SeqCst)
        }

        pub fn joined_subscriptions(&self) -> usize {
            self.joined_subscriptions.load(Ordering::SeqCst)
        }

        pub fn snapshot_requests(&self) -> usize {
            self.snapshot_requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for HardeningHerdr {
        fn drop(&mut self) {
            self.accept_task.abort();
        }
    }

    async fn handle_connection(
        stream: UnixStream,
        config: &HardeningConfig,
        subscriptions: &AtomicUsize,
        active: &AtomicUsize,
        joined: &AtomicUsize,
        snapshots: &AtomicUsize,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let request: Value = serde_json::from_str(&line).map_err(invalid_data)?;
        let id = request["id"]
            .as_str()
            .ok_or_else(|| io::Error::other("missing request id"))?;
        match request["method"].as_str() {
            Some("events.subscribe") => {
                subscriptions.fetch_add(1, Ordering::SeqCst);
                if !config.hang_subscription_ack {
                    write_frame(
                        reader.get_mut(),
                        &json!({"id": id, "result": {"type": "subscription_started"}}),
                    )
                    .await?;
                }
                active.fetch_add(1, Ordering::SeqCst);
                line.clear();
                let result = reader.read_line(&mut line).await;
                active.fetch_sub(1, Ordering::SeqCst);
                joined.fetch_add(1, Ordering::SeqCst);
                result.map(|_| ())
            }
            Some("session.snapshot") => {
                let index = snapshots.fetch_add(1, Ordering::SeqCst);
                match config.replies.get(index).or_else(|| config.replies.last()) {
                    Some(SnapshotReply::Snapshot(snapshot)) => {
                        write_frame(
                            reader.get_mut(),
                            &json!({"id": id, "result": {"type": "session_snapshot", "snapshot": snapshot}}),
                        )
                        .await
                    }
                    Some(SnapshotReply::Error) | None => {
                        write_frame(
                            reader.get_mut(),
                            &json!({"id": id, "error": {"code": "SNAPSHOT_FAILED", "message": "fabricated first snapshot failure"}}),
                        )
                        .await
                    }
                }
            }
            _ => Ok(()),
        }
    }

    async fn write_frame(stream: &mut UnixStream, frame: &Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(frame).map_err(invalid_data)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.flush().await
    }

    fn invalid_data(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
    }
}

// Live-push mock appended for convergence tests that must observe state after LIVE.
#[allow(dead_code)]
pub mod live_mock {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug, Default)]
    pub struct LiveConfig {
        snapshots: Vec<Value>,
    }

    impl LiveConfig {
        pub fn snapshots(mut self, snapshots: Vec<Value>) -> Self {
            self.snapshots = snapshots;
            self
        }
    }

    pub struct LiveHerdr {
        _temp_dir: TempDir,
        socket_path: PathBuf,
        snapshot_requests: Arc<AtomicUsize>,
        active_subscription: Arc<Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>>,
        accept_task: JoinHandle<()>,
    }

    impl LiveHerdr {
        pub async fn start(config: LiveConfig) -> io::Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let socket_path = temp_dir.path().join("herdr.sock");
            let listener = UnixListener::bind(&socket_path)?;
            let snapshot_requests = Arc::new(AtomicUsize::new(0));
            let active_subscription = Arc::new(Mutex::new(None));
            let task_snapshots = Arc::clone(&snapshot_requests);
            let task_subscription = Arc::clone(&active_subscription);
            let config = Arc::new(config);
            let accept_task = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let config = Arc::clone(&config);
                    let snapshots = Arc::clone(&task_snapshots);
                    let subscription = Arc::clone(&task_subscription);
                    tokio::spawn(async move {
                        let _ = handle_connection(stream, &config, &snapshots, &subscription).await;
                    });
                }
            });
            Ok(Self {
                _temp_dir: temp_dir,
                socket_path,
                snapshot_requests,
                active_subscription,
                accept_task,
            })
        }

        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        pub fn snapshot_requests(&self) -> usize {
            self.snapshot_requests.load(Ordering::SeqCst)
        }

        pub async fn push(&self, frame: Value) -> io::Result<()> {
            let sender = self
                .active_subscription
                .lock()
                .map_err(|_| io::Error::other("live subscription mutex was poisoned"))?
                .clone()
                .ok_or_else(|| io::Error::other("live subscription is not connected"))?;
            let (acknowledgement, response) = oneshot::channel();
            sender
                .send(SubscriptionCommand::Push(frame, acknowledgement))
                .map_err(|_| io::Error::other("live subscription is closed"))?;
            response
                .await
                .map_err(|_| io::Error::other("live push was not acknowledged"))
        }
    }

    impl Drop for LiveHerdr {
        fn drop(&mut self) {
            self.accept_task.abort();
        }
    }

    enum SubscriptionCommand {
        Push(Value, oneshot::Sender<()>),
    }

    async fn handle_connection(
        stream: UnixStream,
        config: &LiveConfig,
        snapshot_requests: &AtomicUsize,
        active_subscription: &Mutex<Option<mpsc::UnboundedSender<SubscriptionCommand>>>,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let request: Value = serde_json::from_str(&line).map_err(invalid_data)?;
        let id = request["id"]
            .as_str()
            .ok_or_else(|| io::Error::other("missing request id"))?;
        match request["method"].as_str() {
            Some("events.subscribe") => {
                let (sender, mut commands) = mpsc::unbounded_channel();
                *active_subscription
                    .lock()
                    .map_err(|_| io::Error::other("live subscription mutex was poisoned"))? =
                    Some(sender);
                write_frame(
                    reader.get_mut(),
                    &json!({"id": id, "result": {"type": "subscription_started"}}),
                )
                .await?;
                while let Some(SubscriptionCommand::Push(frame, acknowledgement)) =
                    commands.recv().await
                {
                    write_frame(reader.get_mut(), &frame).await?;
                    let _ = acknowledgement.send(());
                }
                Ok(())
            }
            Some("session.snapshot") => {
                let index = snapshot_requests.fetch_add(1, Ordering::SeqCst);
                let snapshot = config
                    .snapshots
                    .get(index)
                    .or_else(|| config.snapshots.last())
                    .ok_or_else(|| io::Error::other("live mock has no snapshot"))?;
                write_frame(
                    reader.get_mut(),
                    &json!({"id": id, "result": {"type": "session_snapshot", "snapshot": snapshot}}),
                )
                .await
            }
            _ => Ok(()),
        }
    }

    async fn write_frame(stream: &mut UnixStream, frame: &Value) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(frame).map_err(invalid_data)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.flush().await
    }

    fn invalid_data(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
    }
}
