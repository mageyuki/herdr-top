#![allow(unsafe_code)]

//! Privacy-safe compatibility and executable-version probes.

use std::cmp::Ordering;
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::herdr::types::{AgentManifestStatus, Pong};
use crate::model::Provider;

/// Reviewed Herdr socket protocols.
/// Herdr 0.8.0 uses 19; 0.8.2 uses 20 with an additive-only schema change.
pub const SUPPORTED_HERDR_PROTOCOLS: [u32; 2] = [19, 20];
/// Oldest Herdr release compatible with the typed remote probes.
pub const MINIMUM_HERDR_VERSION: &str = "0.8.0";
/// Default wall-clock limit for one external version command.
pub const DEFAULT_VERSION_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
/// Maximum retained bytes from each child output pipe.
pub const VERSION_OUTPUT_LIMIT: usize = 4096;

const PIPE_POLL_INTERVAL: Duration = Duration::from_millis(5);
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Closed reason that a typed Herdr handshake is not compatible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HerdrCompatibilityIssue {
    InvalidVersion,
    VersionTooOld,
    ProtocolMismatch,
}

/// Compatibility derived only from a typed Herdr Pong.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HerdrCompatibility {
    Compatible {
        version: String,
    },
    Unavailable {
        reason: HerdrCompatibilityIssue,
        version: Option<String>,
    },
}

impl HerdrCompatibility {
    /// Returns the closed incompatibility reason, if any.
    #[must_use]
    pub const fn issue(&self) -> Option<HerdrCompatibilityIssue> {
        match self {
            Self::Compatible { .. } => None,
            Self::Unavailable { reason, .. } => Some(*reason),
        }
    }
}

/// Evaluates Herdr compatibility using the minimum version floor and reviewed protocol set.
#[must_use]
pub fn assess_herdr_compatibility(pong: &Pong) -> HerdrCompatibility {
    let Some(version) = NumericVersion::parse(&pong.version) else {
        return HerdrCompatibility::Unavailable {
            reason: HerdrCompatibilityIssue::InvalidVersion,
            version: None,
        };
    };
    if !version.is_at_least(MINIMUM_HERDR_VERSION) {
        return HerdrCompatibility::Unavailable {
            reason: HerdrCompatibilityIssue::VersionTooOld,
            version: Some(version.normalized),
        };
    }
    if !SUPPORTED_HERDR_PROTOCOLS.contains(&pong.protocol) {
        return HerdrCompatibility::Unavailable {
            reason: HerdrCompatibilityIssue::ProtocolMismatch,
            version: Some(version.normalized),
        };
    }
    HerdrCompatibility::Compatible {
        version: version.normalized,
    }
}

/// Closed reason that an official provider integration is unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialIntegrationUnavailableReason {
    MissingManifest,
    MissingActiveVersion,
    InvalidActiveVersion,
    DuplicateManifest,
    BelowMinimum,
}

/// Status of one known provider's official native-session integration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialIntegrationStatus {
    Compatible,
    Unavailable {
        reason: OfficialIntegrationUnavailableReason,
    },
}

/// Complete stable assessment consumed without reopening raw manifest data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialIntegrationAssessment {
    pub provider: Provider,
    pub active_version: Option<String>,
    pub minimum_version: String,
    pub status: OfficialIntegrationStatus,
}

/// Evaluates only the matching manifest's `active_version` field.
#[must_use]
pub fn assess_official_integration(
    status: &AgentManifestStatus,
    provider: Provider,
) -> OfficialIntegrationAssessment {
    let (agent, minimum) = match provider {
        Provider::Claude => ("claude", "6"),
        Provider::Codex => ("codex", "5"),
    };
    let assessment = |active_version, status| OfficialIntegrationAssessment {
        provider,
        active_version,
        minimum_version: minimum.to_owned(),
        status,
    };
    let mut matching = status
        .manifests
        .iter()
        .filter(|manifest| manifest.agent == agent);
    let Some(manifest) = matching.next() else {
        return assessment(
            None,
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::MissingManifest,
            },
        );
    };
    if matching.next().is_some() {
        return assessment(
            None,
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::DuplicateManifest,
            },
        );
    };
    let Some(active_version) = manifest.active_version.as_deref() else {
        return assessment(
            None,
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::MissingActiveVersion,
            },
        );
    };
    let active_version = match classify_active_version(active_version) {
        Some(ActiveVersionForm::LegacyInteger(active_version)) => active_version,
        Some(ActiveVersionForm::DateEra(active_version)) => {
            return assessment(Some(active_version), OfficialIntegrationStatus::Compatible);
        }
        None => {
            return assessment(
                None,
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::InvalidActiveVersion,
                },
            );
        }
    };
    if compare_numeric_component(&active_version, minimum).is_lt() {
        return assessment(
            Some(active_version),
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::BelowMinimum,
            },
        );
    }
    assessment(Some(active_version), OfficialIntegrationStatus::Compatible)
}

/// One of the only external commands version probes may execute.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VersionCommand {
    Standalone,
    Claude,
    Codex,
}

/// Closed reason an external version could not be safely obtained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionProbeFailure {
    NotFound,
    SpawnFailed,
    TimedOut,
    ExecutionFailed,
    UnsuccessfulExit,
    OutputReadFailed,
    OutputTooLarge,
    InvalidOutput,
}

/// Privacy-safe result of one external version command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionProbeResult {
    Available {
        version: String,
        stderr_present: bool,
    },
    Unavailable {
        reason: VersionProbeFailure,
    },
}

/// Injectable boundary used by Doctor and first-launch compatibility policy.
pub trait VersionCommandRunner {
    fn run(&self, command: VersionCommand) -> VersionProbeResult;
}

/// Production runner with bounded time and independently capped pipe capture.
#[derive(Clone, Copy, Debug)]
pub struct BoundedVersionCommandRunner {
    timeout: Duration,
}

impl BoundedVersionCommandRunner {
    /// Returns the configured wall-clock limit.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for BoundedVersionCommandRunner {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_VERSION_COMMAND_TIMEOUT,
        }
    }
}

impl VersionCommandRunner for BoundedVersionCommandRunner {
    fn run(&self, command: VersionCommand) -> VersionProbeResult {
        let executable = match command {
            VersionCommand::Standalone => "herdr-top",
            VersionCommand::Claude => "claude",
            VersionCommand::Codex => "codex",
        };
        let mut process = Command::new(executable);
        process.arg("--version");
        classify_command_outcome(execute_command(&mut process, self.timeout))
    }
}

/// Compatibility of a separately PATH-discovered `herdr-top` executable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StandaloneVersionStatus {
    Compatible {
        version: String,
        stderr_present: bool,
    },
    Mismatch {
        version: String,
        stderr_present: bool,
    },
    Unavailable {
        reason: VersionProbeFailure,
    },
}

/// Applies the exact-package-version standalone policy.
#[must_use]
pub fn standalone_version_status(runner: &impl VersionCommandRunner) -> StandaloneVersionStatus {
    match runner.run(VersionCommand::Standalone) {
        VersionProbeResult::Available {
            version,
            stderr_present,
        } => {
            let Some(normalized) = normalize_semver_core(&version) else {
                return StandaloneVersionStatus::Unavailable {
                    reason: VersionProbeFailure::InvalidOutput,
                };
            };
            if normalized == env!("CARGO_PKG_VERSION") {
                StandaloneVersionStatus::Compatible {
                    version: normalized,
                    stderr_present,
                }
            } else {
                StandaloneVersionStatus::Mismatch {
                    version: normalized,
                    stderr_present,
                }
            }
        }
        VersionProbeResult::Unavailable { reason } => {
            StandaloneVersionStatus::Unavailable { reason }
        }
    }
}

/// Informational-only status of one known provider CLI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderCliVersionStatus {
    Informational {
        version: String,
        stderr_present: bool,
    },
    Unavailable {
        reason: VersionProbeFailure,
    },
}

/// Queries one provider CLI without imposing an age or minimum-version policy.
#[must_use]
pub fn provider_cli_version_status(
    runner: &impl VersionCommandRunner,
    provider: Provider,
) -> ProviderCliVersionStatus {
    let command = match provider {
        Provider::Claude => VersionCommand::Claude,
        Provider::Codex => VersionCommand::Codex,
    };
    match runner.run(command) {
        VersionProbeResult::Available {
            version,
            stderr_present,
        } => match normalize_semver_core(&version) {
            Some(version) => ProviderCliVersionStatus::Informational {
                version,
                stderr_present,
            },
            None => ProviderCliVersionStatus::Unavailable {
                reason: VersionProbeFailure::InvalidOutput,
            },
        },
        VersionProbeResult::Unavailable { reason } => {
            ProviderCliVersionStatus::Unavailable { reason }
        }
    }
}

#[derive(Debug)]
struct CappedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

#[derive(Debug)]
struct DrainOutcome {
    output: CappedOutput,
    deadline_reached: bool,
}

#[derive(Debug)]
enum RawCommandOutcome {
    Completed {
        status: ExitStatus,
        stdout: CappedOutput,
        stderr: CappedOutput,
    },
    SpawnFailed {
        not_found: bool,
    },
    TimedOutAndReaped,
    ExecutionFailed,
    CaptureFailed,
}

fn execute_command(command: &mut Command, timeout: Duration) -> RawCommandOutcome {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let deadline = Instant::now() + timeout;
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return RawCommandOutcome::SpawnFailed {
                not_found: error.kind() == io::ErrorKind::NotFound,
            };
        }
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return RawCommandOutcome::CaptureFailed;
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return RawCommandOutcome::CaptureFailed;
    };
    if set_nonblocking(&stdout).is_err() || set_nonblocking(&stderr).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        drop(stdout);
        drop(stderr);
        return RawCommandOutcome::CaptureFailed;
    }
    let stdout_drain = thread::spawn(move || drain_capped(stdout, deadline));
    let stderr_drain = thread::spawn(move || drain_capped(stderr, deadline));
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if Instant::now() >= deadline => {
                timed_out = true;
                let _ = child.kill();
                break child.wait().map_err(|_| ());
            }
            Ok(None) => sleep_until(deadline, CHILD_POLL_INTERVAL),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(());
            }
        }
    };

    let stdout = stdout_drain.join().ok().and_then(Result::ok);
    let stderr = stderr_drain.join().ok().and_then(Result::ok);
    if timed_out {
        return if status.is_ok() {
            RawCommandOutcome::TimedOutAndReaped
        } else {
            RawCommandOutcome::ExecutionFailed
        };
    }
    let Ok(status) = status else {
        return RawCommandOutcome::ExecutionFailed;
    };
    match (stdout, stderr) {
        (Some(stdout), Some(stderr)) if stdout.deadline_reached || stderr.deadline_reached => {
            RawCommandOutcome::TimedOutAndReaped
        }
        (Some(stdout), Some(stderr)) => RawCommandOutcome::Completed {
            status,
            stdout: stdout.output,
            stderr: stderr.output,
        },
        _ => RawCommandOutcome::CaptureFailed,
    }
}

fn set_nonblocking(descriptor: &impl AsRawFd) -> io::Result<()> {
    let descriptor = descriptor.as_raw_fd();
    // SAFETY: fcntl is called with a live captured pipe descriptor and the argument required by
    // each command. Return values are checked before the descriptor is used by a drainer.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor remains live, the existing flags are preserved, and F_SETFL accepts
    // the integer flag word supplied here. The result is checked.
    if unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn drain_capped(mut reader: impl Read, deadline: Instant) -> io::Result<DrainOutcome> {
    let mut bytes = Vec::with_capacity(VERSION_OUTPUT_LIMIT);
    let mut truncated = false;
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        if Instant::now() >= deadline {
            return Ok(DrainOutcome {
                output: CappedOutput { bytes, truncated },
                deadline_reached: true,
            });
        }
        match reader.read(&mut buffer) {
            Ok(0) => {
                return Ok(DrainOutcome {
                    output: CappedOutput { bytes, truncated },
                    deadline_reached: false,
                });
            }
            Ok(read) => {
                let retained = VERSION_OUTPUT_LIMIT.saturating_sub(bytes.len()).min(read);
                bytes.extend_from_slice(&buffer[..retained]);
                truncated |= retained < read;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                sleep_until(deadline, PIPE_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                sleep_until(deadline, PIPE_POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }
}

fn sleep_until(deadline: Instant, interval: Duration) {
    if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        thread::sleep(remaining.min(interval));
    }
}

fn classify_command_outcome(outcome: RawCommandOutcome) -> VersionProbeResult {
    let (status, stdout, stderr) = match outcome {
        RawCommandOutcome::Completed {
            status,
            stdout,
            stderr,
        } => (status, stdout, stderr),
        RawCommandOutcome::SpawnFailed { not_found: true } => {
            return VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::NotFound,
            };
        }
        RawCommandOutcome::SpawnFailed { not_found: false } => {
            return VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::SpawnFailed,
            };
        }
        RawCommandOutcome::TimedOutAndReaped => {
            return VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::TimedOut,
            };
        }
        RawCommandOutcome::ExecutionFailed => {
            return VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::ExecutionFailed,
            };
        }
        RawCommandOutcome::CaptureFailed => {
            return VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::OutputReadFailed,
            };
        }
    };
    if !status.success() {
        return VersionProbeResult::Unavailable {
            reason: VersionProbeFailure::UnsuccessfulExit,
        };
    }
    if stdout.truncated {
        return VersionProbeResult::Unavailable {
            reason: VersionProbeFailure::OutputTooLarge,
        };
    }
    let stderr_present = !stderr.bytes.is_empty() || stderr.truncated;
    match parse_tool_version(&stdout.bytes) {
        Some(version) => VersionProbeResult::Available {
            version,
            stderr_present,
        },
        None => VersionProbeResult::Unavailable {
            reason: VersionProbeFailure::InvalidOutput,
        },
    }
}

fn parse_tool_version(stdout: &[u8]) -> Option<String> {
    let stdout = std::str::from_utf8(stdout).ok()?;
    let mut found = None;
    for raw_token in stdout.split_ascii_whitespace() {
        let token = raw_token.trim_matches(|character: char| {
            matches!(
                character,
                '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' | ':'
            )
        });
        let token = token.strip_prefix('v').unwrap_or(token);
        let Some(version) = normalize_semver_core(token) else {
            continue;
        };
        if found.replace(version).is_some() {
            return None;
        }
    }
    found
}

struct NumericVersion {
    normalized: String,
    components: [String; 3],
}

impl NumericVersion {
    fn parse(value: &str) -> Option<Self> {
        let normalized = normalize_semver_core(value)?;
        let mut parts = normalized.split('.');
        let components = [
            parts.next()?.to_owned(),
            parts.next()?.to_owned(),
            parts.next()?.to_owned(),
        ];
        Some(Self {
            normalized,
            components,
        })
    }

    fn is_at_least(&self, minimum: &str) -> bool {
        let minimum = Self::parse(minimum).expect("minimum version constant must be canonical");
        self.components
            .iter()
            .zip(minimum.components.iter())
            .map(|(left, right)| compare_numeric_component(left, right))
            .find(|ordering| !ordering.is_eq())
            .unwrap_or(Ordering::Equal)
            .is_ge()
    }
}

fn normalize_semver_core(value: &str) -> Option<String> {
    let mut parts = value.split('.');
    let components = [parts.next()?, parts.next()?, parts.next()?];
    if parts.next().is_some()
        || components
            .iter()
            .any(|component| normalize_integer(component).is_none())
    {
        return None;
    }
    Some(components.join("."))
}

/// Recognized forms of an official integration's active version.
enum ActiveVersionForm {
    LegacyInteger(String),
    DateEra(String),
}

/// Classifies legacy integer and newer date-era active versions.
fn classify_active_version(value: &str) -> Option<ActiveVersionForm> {
    if let Some(integer) = normalize_integer(value) {
        return Some(ActiveVersionForm::LegacyInteger(integer));
    }
    let components: Vec<&str> = value.split('.').collect();
    let date_era = components.len() >= 2
        && components
            .iter()
            .all(|c| !c.is_empty() && c.bytes().all(|b| b.is_ascii_digit()));
    date_era.then(|| ActiveVersionForm::DateEra(value.to_owned()))
}

fn normalize_integer(value: &str) -> Option<String> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    Some(value.to_owned())
}

fn compare_numeric_component(left: &str, right: &str) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env;
    use std::ffi::OsStr;
    use std::fs;
    use std::io::{self, Write};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use crate::herdr::types::{AgentManifestInfo, AgentManifestStatus, Pong};
    use crate::model::Provider;
    use crate::provider::standard_discovery_roots;

    use super::{
        BoundedVersionCommandRunner, HerdrCompatibility, HerdrCompatibilityIssue,
        OfficialIntegrationAssessment, OfficialIntegrationStatus,
        OfficialIntegrationUnavailableReason, ProviderCliVersionStatus, RawCommandOutcome,
        StandaloneVersionStatus, VersionCommand, VersionCommandRunner, VersionProbeFailure,
        VersionProbeResult, assess_herdr_compatibility, assess_official_integration,
        classify_command_outcome, execute_command, parse_tool_version, provider_cli_version_status,
        standalone_version_status,
    };

    const CHILD_MODE: &str = "HERDR_TOP_REMOTE_TEST_CHILD_MODE";
    const CHILD_PID_PATH: &str = "HERDR_TOP_REMOTE_TEST_CHILD_PID_PATH";

    // This bounded fixture must outlive its direct parent to retain the inherited pipes.
    #[allow(clippy::zombie_processes)]
    fn spawn_pipe_holding_descendant() {
        Command::new(env::current_exe().unwrap())
            .args([
                OsStr::new("--exact"),
                OsStr::new("diagnostics::remote::tests::remote_command_child_helper"),
                OsStr::new("--nocapture"),
            ])
            .env(CHILD_MODE, "pipe-descendant")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("controlled pipe-holding descendant should start");
    }

    #[test]
    fn remote_command_child_helper() {
        let Some(mode) = env::var_os(CHILD_MODE) else {
            return;
        };
        match mode.to_str().expect("test child mode should be UTF-8") {
            "hang" => {
                let pid_path = env::var_os(CHILD_PID_PATH).expect("PID path should be supplied");
                fs::write(pid_path, std::process::id().to_string()).unwrap();
                loop {
                    thread::sleep(Duration::from_secs(60));
                }
            }
            "flood" => {
                let block = [b'x'; 8 * 1024];
                for _ in 0..16 {
                    io::stdout().write_all(&block).unwrap();
                }
                io::stdout().flush().unwrap();
                for _ in 0..16 {
                    io::stderr().write_all(&block).unwrap();
                }
                io::stderr().flush().unwrap();
            }
            "safe-stderr" => {
                writeln!(io::stdout(), "Claude Code 6.1.2").unwrap();
                writeln!(io::stderr(), "private provider warning text").unwrap();
            }
            "hold-pipes" => spawn_pipe_holding_descendant(),
            "pipe-descendant" => thread::sleep(Duration::from_secs(2)),
            other => panic!("unknown remote test child mode {other}"),
        }
    }

    #[test]
    fn i4_remote_manifest_null_missing_old_current_future_matrix() {
        let empty = manifest_status(Vec::new());
        assert_eq!(
            assess_official_integration(&empty, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                None,
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::MissingManifest,
                },
            )
        );

        let null = manifest_status(vec![manifest("claude", None)]);
        assert_eq!(
            assess_official_integration(&null, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                None,
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::MissingActiveVersion,
                },
            )
        );
        let zero = manifest_status(vec![manifest("claude", Some("0"))]);
        assert_eq!(
            assess_official_integration(&zero, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                Some("0"),
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::BelowMinimum,
                },
            )
        );

        let invalid = manifest_status(vec![manifest("claude", Some("v6"))]);
        assert_eq!(
            assess_official_integration(&invalid, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                None,
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::InvalidActiveVersion,
                },
            )
        );

        let claude_old = manifest_status(vec![manifest("claude", Some("5"))]);
        assert_eq!(
            assess_official_integration(&claude_old, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                Some("5"),
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::BelowMinimum,
                },
            )
        );
        let claude_current = manifest_status(vec![manifest("claude", Some("6"))]);
        assert_eq!(
            assess_official_integration(&claude_current, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                Some("6"),
                "6",
                OfficialIntegrationStatus::Compatible,
            )
        );

        let codex_matrix = manifest_status(vec![
            manifest("claude", Some("999")),
            manifest("codex", Some("4")),
        ]);
        assert_eq!(
            assess_official_integration(&codex_matrix, Provider::Codex),
            integration_assessment(
                Provider::Codex,
                Some("4"),
                "5",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::BelowMinimum,
                },
            )
        );
        let codex_current = manifest_status(vec![manifest("codex", Some("5"))]);
        assert_eq!(
            assess_official_integration(&codex_current, Provider::Codex),
            integration_assessment(
                Provider::Codex,
                Some("5"),
                "5",
                OfficialIntegrationStatus::Compatible,
            )
        );
        let future = "18446744073709551616000000000000000000";
        let codex_future = manifest_status(vec![manifest("codex", Some(future))]);
        assert_eq!(
            assess_official_integration(&codex_future, Provider::Codex),
            integration_assessment(
                Provider::Codex,
                Some(future),
                "5",
                OfficialIntegrationStatus::Compatible,
            )
        );

        let identical_duplicates = manifest_status(vec![
            manifest("claude", Some("6")),
            manifest("claude", Some("6")),
        ]);
        let conflicting_duplicates = manifest_status(vec![
            manifest("claude", Some("6")),
            manifest("claude", Some("5")),
        ]);
        let reversed_duplicates = manifest_status(vec![
            manifest("claude", Some("5")),
            manifest("claude", Some("6")),
        ]);
        let duplicate_assessment = integration_assessment(
            Provider::Claude,
            None,
            "6",
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::DuplicateManifest,
            },
        );
        for duplicates in [
            identical_duplicates,
            conflicting_duplicates,
            reversed_duplicates,
        ] {
            assert_eq!(
                assess_official_integration(&duplicates, Provider::Claude),
                duplicate_assessment
            );
        }

        let unrelated_provider = manifest_status(vec![
            manifest("claude", Some("1")),
            manifest("claude", Some("6")),
            manifest("codex", Some("5")),
        ]);
        assert_eq!(
            assess_official_integration(&unrelated_provider, Provider::Codex),
            integration_assessment(
                Provider::Codex,
                Some("5"),
                "5",
                OfficialIntegrationStatus::Compatible,
            )
        );

        let misleading_nonactive_fields = AgentManifestStatus {
            result_type: "agent_manifest_status".to_owned(),
            manifests: vec![AgentManifestInfo {
                agent: "claude".to_owned(),
                source: "official-v999".to_owned(),
                source_kind: "cached-999".to_owned(),
                local_override_shadowing_remote: false,
                active_version: None,
                cached_remote_version: Some("999".to_owned()),
                remote_last_checked_unix: None,
                remote_update_error: None,
                remote_update_result: Some("version 999".to_owned()),
                warning: None,
            }],
            last_check_unix: None,
            last_result: None,
        };
        assert_eq!(
            assess_official_integration(&misleading_nonactive_fields, Provider::Claude),
            integration_assessment(
                Provider::Claude,
                None,
                "6",
                OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::MissingActiveVersion,
                },
            )
        );
    }

    #[test]
    fn date_form_active_version_is_compatible_verbatim() {
        let status = manifest_status(vec![manifest("claude", Some("2026.08.12.1"))]);
        let a = assess_official_integration(&status, Provider::Claude);
        assert_eq!(a.status, OfficialIntegrationStatus::Compatible);
        assert_eq!(a.active_version.as_deref(), Some("2026.08.12.1"));
    }

    #[test]
    fn date_form_with_two_components_is_compatible() {
        let status = manifest_status(vec![manifest("codex", Some("2026.8"))]);
        let a = assess_official_integration(&status, Provider::Codex);
        assert_eq!(a.status, OfficialIntegrationStatus::Compatible);
    }

    #[test]
    fn legacy_integer_still_compares_against_minimum() {
        let below = manifest_status(vec![manifest("claude", Some("5"))]);
        assert!(matches!(
            assess_official_integration(&below, Provider::Claude).status,
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::BelowMinimum
            }
        ));
        let ok = manifest_status(vec![manifest("claude", Some("7"))]);
        assert_eq!(
            assess_official_integration(&ok, Provider::Claude).status,
            OfficialIntegrationStatus::Compatible
        );
    }

    #[test]
    fn malformed_versions_stay_invalid() {
        for bad in ["", "v7", "7a", "07", "2026..1", ".1", "1.", "2026.08.x"] {
            let status = manifest_status(vec![manifest("claude", Some(bad))]);
            assert!(
                matches!(
                    assess_official_integration(&status, Provider::Claude).status,
                    OfficialIntegrationStatus::Unavailable {
                        reason: OfficialIntegrationUnavailableReason::InvalidActiveVersion
                    }
                ),
                "{bad:?} must stay invalid"
            );
        }
    }

    #[test]
    fn i4_remote_herdr_version_protocol_matrix() {
        assert_eq!(
            assess_herdr_compatibility(&pong("0.8.0", 19)),
            HerdrCompatibility::Compatible {
                version: "0.8.0".to_owned(),
            }
        );
        assert_eq!(
            assess_herdr_compatibility(&pong("1.25.300", 19)),
            HerdrCompatibility::Compatible {
                version: "1.25.300".to_owned(),
            }
        );
        assert_eq!(
            assess_herdr_compatibility(&pong("0.8.0", 20)),
            HerdrCompatibility::Compatible {
                version: "0.8.0".to_owned(),
            }
        );
        assert_eq!(
            assess_herdr_compatibility(&pong("1.25.300", 20)),
            HerdrCompatibility::Compatible {
                version: "1.25.300".to_owned(),
            }
        );
        assert!(matches!(
            assess_herdr_compatibility(&pong("999999999999999999999999.25.300", 19)),
            HerdrCompatibility::Compatible { .. }
        ));
        for version in ["0.7.999", "0.8", "v0.8.0", "0.8.0-beta", "0.8.0+build"] {
            let expected = if version == "0.7.999" {
                HerdrCompatibilityIssue::VersionTooOld
            } else {
                HerdrCompatibilityIssue::InvalidVersion
            };
            assert_eq!(
                assess_herdr_compatibility(&pong(version, 19)).issue(),
                Some(expected),
                "unexpected result for {version}"
            );
        }
        for protocol in [0, 18, 21, u32::MAX] {
            assert_eq!(
                assess_herdr_compatibility(&pong("99.0.0", protocol)).issue(),
                Some(HerdrCompatibilityIssue::ProtocolMismatch)
            );
        }
    }

    #[test]
    fn i4_remote_provider_roots_are_shared_pure_absolute_only() {
        assert!(standard_discovery_roots(None).is_empty());
        assert!(standard_discovery_roots(Some(OsStr::new(""))).is_empty());
        assert!(standard_discovery_roots(Some(OsStr::new("relative/home"))).is_empty());

        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("missing-home");
        assert_eq!(
            standard_discovery_roots(Some(home.as_os_str())),
            vec![
                crate::provider::DiscoveryRoot {
                    provider: Provider::Claude,
                    path: home.join(".claude/projects"),
                },
                crate::provider::DiscoveryRoot {
                    provider: Provider::Codex,
                    path: home.join(".codex/sessions"),
                },
            ]
        );
        assert!(
            !home.exists(),
            "pure root derivation must not probe or create"
        );
    }

    #[test]
    fn i4_remote_timeout_kills_and_reaps() {
        let directory = tempfile::tempdir().unwrap();
        let pid_path = directory.path().join("child.pid");
        let mut command = helper_command("hang");
        command.env(CHILD_PID_PATH, &pid_path);

        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let outcome = execute_command(&mut command, Duration::from_millis(500));
            sender.send(outcome).unwrap();
        });
        let outcome = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("timed command must complete within the bounded test wait");
        worker.join().unwrap();
        assert!(matches!(outcome, RawCommandOutcome::TimedOutAndReaped));

        fs::read_to_string(&pid_path)
            .expect("controlled looping child should publish its PID before timeout");
    }

    #[test]
    fn i4_remote_bounds_both_pipes_without_deadlock() {
        let mut command = helper_command("flood");
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let outcome = execute_command(&mut command, Duration::from_secs(3));
            sender.send(outcome).unwrap();
        });
        let outcome = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("concurrent pipe drains must not deadlock");
        worker.join().unwrap();

        let RawCommandOutcome::Completed {
            status,
            stdout,
            stderr,
        } = outcome
        else {
            panic!("flood child should complete normally");
        };
        assert!(status.success());
        assert_eq!(stdout.bytes.len(), 4096);
        assert_eq!(stderr.bytes.len(), 4096);
        assert!(stdout.truncated);
        assert!(stderr.truncated);
        assert_eq!(
            classify_command_outcome(RawCommandOutcome::Completed {
                status,
                stdout,
                stderr,
            }),
            VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::OutputTooLarge,
            }
        );
    }

    #[test]
    fn i4_remote_descendant_held_pipes_respect_global_deadline() {
        let mut command = helper_command("hold-pipes");
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let result =
                classify_command_outcome(execute_command(&mut command, Duration::from_millis(500)));
            sender.send(result).unwrap();
        });

        let result = match receiver.recv_timeout(Duration::from_millis(1_500)) {
            Ok(result) => result,
            Err(error) => {
                let late_result = receiver
                    .recv_timeout(Duration::from_secs(4))
                    .expect("self-terminating descendant must release both pipes");
                worker.join().unwrap();
                panic!(
                    "runner exceeded its global deadline ({error}); late result: {late_result:?}"
                );
            }
        };
        worker.join().unwrap();
        assert_eq!(
            result,
            VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::TimedOut,
            }
        );
    }

    #[test]
    fn i4_remote_runner_closed_outcome_matrix() {
        for script in ["exit 7", "kill -TERM $$"] {
            let mut command = shell_command(script);
            assert_eq!(
                classify_command_outcome(execute_command(&mut command, Duration::from_secs(2),)),
                VersionProbeResult::Unavailable {
                    reason: VersionProbeFailure::UnsuccessfulExit,
                },
                "nonzero and signal exits must share the closed exit result"
            );
        }

        for script in [
            "exit 0",
            "printf 'not-a-version\\n'",
            "printf 'tool 1.2.3 and 4.5.6\\n'",
        ] {
            let mut command = shell_command(script);
            assert_eq!(
                classify_command_outcome(execute_command(&mut command, Duration::from_secs(2),)),
                VersionProbeResult::Unavailable {
                    reason: VersionProbeFailure::InvalidOutput,
                },
                "empty, invalid, and ambiguous stdout must fail closed"
            );
        }

        let directory = tempfile::tempdir().unwrap();
        let mut command = Command::new(directory.path().join("guaranteed-not-found"));
        assert_eq!(
            classify_command_outcome(execute_command(&mut command, Duration::from_secs(2),)),
            VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::NotFound,
            }
        );
    }

    #[test]
    fn i4_remote_parseable_stdout_with_stderr_is_safe_success() {
        let mut command = helper_command("safe-stderr");
        let outcome = execute_command(&mut command, Duration::from_secs(3));
        let result = classify_command_outcome(outcome);
        assert_eq!(
            result,
            VersionProbeResult::Available {
                version: "6.1.2".to_owned(),
                stderr_present: true,
            }
        );
        assert!(!format!("{result:?}").contains("private provider warning text"));
    }

    #[test]
    fn i4_remote_standalone_exact_and_cli_informational_matrix() {
        let package = env!("CARGO_PKG_VERSION").to_owned();
        let exact = FakeRunner::available(VersionCommand::Standalone, &package, true);
        assert_eq!(
            standalone_version_status(&exact),
            StandaloneVersionStatus::Compatible {
                version: package.clone(),
                stderr_present: true,
            }
        );
        assert_eq!(exact.commands(), vec![VersionCommand::Standalone]);

        let mismatch = FakeRunner::available(VersionCommand::Standalone, "999.0.0", false);
        assert_eq!(
            standalone_version_status(&mismatch),
            StandaloneVersionStatus::Mismatch {
                version: "999.0.0".to_owned(),
                stderr_present: false,
            }
        );

        let unavailable =
            FakeRunner::unavailable(VersionCommand::Standalone, VersionProbeFailure::NotFound);
        assert_eq!(
            standalone_version_status(&unavailable),
            StandaloneVersionStatus::Unavailable {
                reason: VersionProbeFailure::NotFound,
            }
        );

        for (provider, command) in [
            (Provider::Claude, VersionCommand::Claude),
            (Provider::Codex, VersionCommand::Codex),
        ] {
            let old = FakeRunner::available(command, "0.0.1", false);
            assert_eq!(
                provider_cli_version_status(&old, provider),
                ProviderCliVersionStatus::Informational {
                    version: "0.0.1".to_owned(),
                    stderr_present: false,
                }
            );
            assert_eq!(old.commands(), vec![command]);
        }

        assert_eq!(
            parse_tool_version(b"Claude Code v6.1.2\n"),
            Some("6.1.2".to_owned())
        );
        assert_eq!(
            parse_tool_version(b"codex-cli (5.7.9)\n"),
            Some("5.7.9".to_owned())
        );
        assert_eq!(parse_tool_version(b"tool 1.2.3 and 4.5.6\n"), None);
        assert_eq!(parse_tool_version(b"tool release-1.2.3\n"), None);
        assert_eq!(parse_tool_version(b"tool 01.2.3\n"), None);
        assert_eq!(parse_tool_version(b"tool 1.2.3-beta\n"), None);
        assert_eq!(parse_tool_version(b"tool 1.2.3+build\n"), None);

        assert_eq!(
            BoundedVersionCommandRunner::default().timeout(),
            Duration::from_secs(2)
        );
    }

    fn pong(version: &str, protocol: u32) -> Pong {
        Pong {
            result_type: "pong".to_owned(),
            version: version.to_owned(),
            protocol,
            capabilities: None,
        }
    }

    fn manifest_status(manifests: Vec<AgentManifestInfo>) -> AgentManifestStatus {
        AgentManifestStatus {
            result_type: "agent_manifest_status".to_owned(),
            manifests,
            last_check_unix: None,
            last_result: None,
        }
    }

    fn manifest(agent: &str, active_version: Option<&str>) -> AgentManifestInfo {
        AgentManifestInfo {
            agent: agent.to_owned(),
            source: "/private/source-must-not-be-used".to_owned(),
            source_kind: "remote".to_owned(),
            local_override_shadowing_remote: false,
            active_version: active_version.map(str::to_owned),
            cached_remote_version: Some("999".to_owned()),
            remote_last_checked_unix: Some(1),
            remote_update_error: None,
            remote_update_result: None,
            warning: None,
        }
    }

    fn integration_assessment(
        provider: Provider,
        active_version: Option<&str>,
        minimum_version: &str,
        status: OfficialIntegrationStatus,
    ) -> OfficialIntegrationAssessment {
        OfficialIntegrationAssessment {
            provider,
            active_version: active_version.map(str::to_owned),
            minimum_version: minimum_version.to_owned(),
            status,
        }
    }

    fn helper_command(mode: &str) -> Command {
        let mut command = Command::new(env::current_exe().unwrap());
        command
            .args([
                OsStr::new("--exact"),
                OsStr::new("diagnostics::remote::tests::remote_command_child_helper"),
                OsStr::new("--nocapture"),
            ])
            .env(CHILD_MODE, mode);
        command
    }

    fn shell_command(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        command
    }

    #[derive(Default)]
    struct FakeRunner {
        results: HashMap<VersionCommand, VersionProbeResult>,
        commands: std::sync::Mutex<Vec<VersionCommand>>,
    }

    impl FakeRunner {
        fn available(command: VersionCommand, version: &str, stderr_present: bool) -> Self {
            Self {
                results: HashMap::from([(
                    command,
                    VersionProbeResult::Available {
                        version: version.to_owned(),
                        stderr_present,
                    },
                )]),
                commands: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn unavailable(command: VersionCommand, reason: VersionProbeFailure) -> Self {
            Self {
                results: HashMap::from([(command, VersionProbeResult::Unavailable { reason })]),
                commands: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn commands(&self) -> Vec<VersionCommand> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl VersionCommandRunner for FakeRunner {
        fn run(&self, command: VersionCommand) -> VersionProbeResult {
            self.commands.lock().unwrap().push(command);
            self.results
                .get(&command)
                .expect("fake result should cover requested command")
                .clone()
        }
    }
}
