//! Shared integration-test helpers.

use std::ffi::OsStr;
use std::process::{Command, Output};

/// Re-executes this integration-test binary and runs one exact helper test.
///
/// Re-exec avoids `fork` in libtest's threaded process and lets tests exercise
/// behavior that must cross an OS process boundary.
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
