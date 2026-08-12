#![deny(unsafe_code)]

use std::env;
use std::ffi::OsStr;
use std::fs::{OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use herdr_top::diagnostics::local::{self, BreadcrumbPublishError};
use herdr_top::diagnostics::{PersistenceOccurrenceSink, SharedFileOccurrenceSink};
use herdr_top::doctor::{self, DoctorVersionRunner};
use herdr_top::herdr::collector::{self, CollectorError, SourceAvailability};
use herdr_top::herdr::controller::{self, ControllerEnvelope, EmitOutcome};
use herdr_top::herdr::wire;
use herdr_top::lockfile::{self, LockError, OwnerRecord, StateRoot};
use herdr_top::rendezvous::{self, RvError};
use herdr_top::session_key::{self, ResolvedSession, SessionKeyError};
use herdr_top::store::{self, StoreError, WriterError};
use herdr_top::tui::app::{App, HeaderInputs};
use serde_json::json;
use thiserror::Error;

const OWNER_STARTING_RETRIES: usize = 5;
const OWNER_STARTING_DELAY: Duration = Duration::from_millis(200);
const LOG_FILE: &str = "herdr-top.log";
const LOG_FILE_MODE: u32 = 0o600;

#[derive(Debug, Parser)]
#[command(name = "herdr-top", version)]
struct Cli {
    /// Exact Herdr named session to monitor.
    #[arg(long, global = true)]
    session: Option<String>,
    /// Override the Herdr connection socket; requires an explicit session name.
    #[arg(long, global = true, value_name = "PATH", requires = "session")]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Emit(Box<EmitArgs>),
    Doctor(DoctorArgs),
}

#[derive(Debug, Args)]
struct DoctorArgs {
    /// Render the fixed Doctor JSON schema v1.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct EmitArgs {
    /// Return failure unless the event is accepted or already present.
    #[arg(long)]
    strict: bool,
    /// Controller wire schema version.
    #[arg(long, default_value_t = 1)]
    schema_version: u64,
    #[arg(long)]
    event_id: String,
    #[arg(long)]
    emitted_at_ms: i64,
    #[arg(long)]
    source: String,
    #[arg(long)]
    event_type: String,
    #[arg(long)]
    task_run_id: String,
    #[arg(long)]
    parent_task_run_id: Option<String>,
    #[arg(long)]
    depends_on_id: Option<String>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    progress: Option<f64>,
    #[arg(long)]
    provider: Option<String>,
    #[arg(long)]
    native_session_id: Option<String>,
    #[arg(long)]
    terminal_id: Option<String>,
}

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    Session(#[from] SessionKeyError),
    #[error(transparent)]
    Lock(#[from] LockError),
    #[error(transparent)]
    Rendezvous(#[from] RvError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Writer(#[from] WriterError),
    #[error(transparent)]
    Collector(#[from] CollectorError),
    #[error(transparent)]
    Tui(#[from] io::Error),
    #[error("TUI blocking task failed: {0}")]
    TuiTask(String),
    #[error("HERDR_SOCKET_PATH is unset or empty; pass --socket with --session")]
    MissingSocket,
    #[error("collector startup failed: {startup}; writer shutdown also failed: {shutdown}")]
    StartupShutdown {
        startup: Box<CollectorError>,
        shutdown: Box<WriterError>,
    },
    #[error("failed to clone the bound Controller listener: {0}")]
    ControllerListener(#[source] io::Error),
    #[error("failed to initialize tracing at {path:?}: {source}")]
    TracingIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to install the tracing subscriber: {0}")]
    TracingInit(String),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let configured_plugin_state_dir = env::var_os("HERDR_PLUGIN_STATE_DIR");
    let plugin_state_dir =
        plugin_state_dir_for_command(cli.command.as_ref(), configured_plugin_state_dir.as_deref());
    let result = match &cli.command {
        None => run_monitor(&cli, plugin_state_dir).await,
        Some(Command::Emit(args)) => return run_emit(&cli, args).await,
        Some(Command::Doctor(args)) => return run_doctor(&cli, args).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("herdr-top: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_emit(cli: &Cli, args: &EmitArgs) -> ExitCode {
    let resolved = match resolve_session(cli) {
        Ok(resolved) => resolved,
        Err(error) => return emit_unavailable(args.strict, error.to_string()),
    };
    let runtime = match rendezvous::open_runtime_dir_for_client() {
        Ok(runtime) => runtime,
        Err(error) => return emit_unavailable(args.strict, error.to_string()),
    };
    let endpoint = match rendezvous::resolve_controller_socket(&runtime, resolved.session_key()) {
        Ok(rendezvous::ControllerEndpointStatus::Available(endpoint)) => endpoint,
        Ok(rendezvous::ControllerEndpointStatus::Unavailable(reason)) => {
            return emit_unavailable(args.strict, format!("{reason:?}"));
        }
        Err(error) => return emit_unavailable(args.strict, error.to_string()),
    };
    let envelope = ControllerEnvelope {
        schema_version: args.schema_version,
        event_id: args.event_id.clone(),
        emitted_at_ms: args.emitted_at_ms,
        source: args.source.clone(),
        event_type: args.event_type.clone(),
        task_run_id: args.task_run_id.clone(),
        parent_task_run_id: args.parent_task_run_id.clone(),
        depends_on_id: args.depends_on_id.clone(),
        label: args.label.clone(),
        reason: args.reason.clone(),
        progress: args.progress,
        provider: args.provider.clone(),
        native_session_id: args.native_session_id.clone(),
        terminal_id: args.terminal_id.clone(),
    };
    let outcome = controller::emit_to_endpoint(&endpoint, &envelope).await;
    match &outcome {
        EmitOutcome::Response(response) => match serde_json::to_string(response) {
            Ok(response) => println!("{response}"),
            Err(error) => eprintln!("herdr-top emit: unresolved: {error}"),
        },
        EmitOutcome::Unresolved(reason) => eprintln!("herdr-top emit: unresolved: {reason}"),
    }
    if args.strict && !outcome.is_success() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn emit_unavailable(strict: bool, reason: String) -> ExitCode {
    eprintln!("herdr-top emit: unavailable: {reason}");
    if strict {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_monitor(cli: &Cli, plugin_state_dir: Option<&OsStr>) -> Result<(), MainError> {
    let resolved = resolve_session(cli)?;
    let root = lockfile::state_root(resolved.session_key())?;
    let occurrence_sink = initialize_tracing(&root)?;
    let (owner_lock, breadcrumb_status) =
        acquire_monitor_lock_with_plugin_dir(&root, plugin_state_dir)?;
    if let BreadcrumbLaunchStatus::Failed(error) = breadcrumb_status {
        eprintln!("{error}");
    }

    let Some(owner_lock) = owner_lock else {
        return run_held_branch(cli, &root).await;
    };

    let socket = herdr_socket(cli)?;
    let runtime = rendezvous::open_runtime_dir()?;
    let controller_status =
        rendezvous::prepare_controller_socket(&runtime, resolved.session_key(), &owner_lock)?;
    eprintln!("controller_socket: {controller_status:?}");
    let controller_listener = controller_status
        .try_clone_listener()
        .map_err(MainError::ControllerListener)?;
    let controller_coverage = match &controller_status {
        rendezvous::ControllerSocketStatus::Bound(_) => SourceAvailability::Available,
        rendezvous::ControllerSocketStatus::Unavailable(reason) => {
            let detail = match reason {
                rendezvous::ControllerInputUnavailable::UnsafeOrphan => "unsafe_orphan",
                rendezvous::ControllerInputUnavailable::SentinelMalformed => "sentinel_malformed",
                rendezvous::ControllerInputUnavailable::Collision { .. } => "collision",
                rendezvous::ControllerInputUnavailable::LiveEndpointUnderLock => {
                    "live_endpoint_under_lock"
                }
                rendezvous::ControllerInputUnavailable::BindFailure(_) => "bind_failure",
                rendezvous::ControllerInputUnavailable::PathTooLong => "path_too_long",
            };
            SourceAvailability::Unavailable {
                detail: detail.to_owned(),
            }
        }
    };

    let _schema = store::preflight_schema(&root)?;
    let store = store::open_writer(&root)?;
    let restored = store.load_restored_state()?;
    let restored_operator = store.load_restored_operator_state()?;
    let (lifecycle, writer) = store::spawn_writer(store)?;
    let session_name = resolved.session_key().name().to_owned();
    let collector =
        match collector::spawn_with_controller_coverage_occurrence_sink_and_operator_seed(
            socket,
            session_name.clone(),
            restored,
            writer,
            controller_listener,
            controller_coverage,
            occurrence_sink,
            restored_operator,
        )
        .await
        {
            Ok(collector) => collector,
            Err(startup) => {
                return match lifecycle.shutdown().await {
                    Ok(()) => Err(MainError::Collector(startup)),
                    Err(shutdown) => Err(MainError::StartupShutdown {
                        startup: Box::new(startup),
                        shutdown: Box::new(shutdown),
                    }),
                };
            }
        };

    let mut app = App::new(
        collector.model.clone(),
        collector.quality.clone(),
        HeaderInputs {
            host: resolve_hostname(),
            session: session_name,
            event_lag: Duration::ZERO,
            source_coverage: collector.source_coverage.clone(),
        },
    );
    let tui_result = tokio::task::spawn_blocking(move || app.run())
        .await
        .map_err(|error| MainError::TuiTask(error.to_string()))
        .and_then(|result| result.map_err(MainError::Tui));
    let collector_result = collector.stop().await;
    let writer_result = lifecycle.shutdown().await;
    rendezvous::shutdown_controller_socket(controller_status, &owner_lock)?;

    tui_result?;
    collector_result?;
    writer_result?;
    Ok(())
}

fn resolve_hostname() -> String {
    rendezvous::gethostname()
        .or_else(|| env::var("HOSTNAME").ok().filter(|value| !value.is_empty()))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn tracing_log_path(root: &StateRoot) -> PathBuf {
    root.0.join(LOG_FILE)
}

fn build_tracing_subscriber(
    root: &StateRoot,
) -> io::Result<(
    impl tracing::Subscriber + Send + Sync,
    Arc<dyn PersistenceOccurrenceSink>,
)> {
    let path = tracing_log_path(root);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(LOG_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.set_permissions(Permissions::from_mode(LOG_FILE_MODE))?;
    let file = Arc::new(Mutex::new(file));
    let shared_log = SharedFileOccurrenceSink::new(Arc::clone(&file));
    let occurrence_sink: Arc<dyn PersistenceOccurrenceSink> = Arc::new(shared_log.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(shared_log)
        .finish();
    Ok((subscriber, occurrence_sink))
}

fn initialize_tracing(root: &StateRoot) -> Result<Arc<dyn PersistenceOccurrenceSink>, MainError> {
    let path = tracing_log_path(root);
    let (subscriber, occurrence_sink) =
        build_tracing_subscriber(root).map_err(|source| MainError::TracingIo { path, source })?;
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| MainError::TracingInit(error.to_string()))?;
    Ok(occurrence_sink)
}

async fn run_held_branch(cli: &Cli, root: &StateRoot) -> Result<(), MainError> {
    let Some(owner) = read_owner_with_retry(root).await? else {
        println!(
            "OwnerStarting: session lock is held and the owner record is not available after {} retries",
            OWNER_STARTING_RETRIES
        );
        return Ok(());
    };

    let focus_result = match herdr_socket(cli) {
        Ok(socket) => focus_existing_owner(&socket, &owner).await,
        Err(error) => Err(error.to_string()),
    };
    match focus_result {
        Ok(pane_id) => println!("focused owner pane {pane_id}"),
        Err(reason) => eprintln!(
            "could not focus existing owner ({reason}); {}",
            owner_information(&owner)
        ),
    }
    Ok(())
}

async fn read_owner_with_retry(root: &StateRoot) -> Result<Option<OwnerRecord>, StoreError> {
    for retry in 0..=OWNER_STARTING_RETRIES {
        match store::open_reader(root) {
            Ok(reader) => {
                if let Some(owner) = reader.read_owner()? {
                    return Ok(Some(owner));
                }
            }
            Err(StoreError::DatabaseAbsent(_) | StoreError::SchemaNotCurrent) => {}
            Err(error) => return Err(error),
        }

        if retry < OWNER_STARTING_RETRIES {
            tokio::time::sleep(OWNER_STARTING_DELAY).await;
        }
    }
    Ok(None)
}

async fn focus_existing_owner(socket: &Path, owner: &OwnerRecord) -> Result<String, String> {
    let terminal_id = owner
        .terminal_id
        .as_deref()
        .ok_or_else(|| "owner record has no terminal_id".to_owned())?;
    let snapshot = wire::request(socket, "session.snapshot", json!({}))
        .await
        .and_then(|result| result.into_snapshot())
        .map_err(|error| error.to_string())?;
    let pane = snapshot
        .panes
        .into_iter()
        .find(|pane| pane.terminal_id == terminal_id)
        .ok_or_else(|| {
            format!("owner terminal {terminal_id:?} is absent from the live snapshot")
        })?;
    wire::request(socket, "pane.focus", json!({"pane_id": pane.pane_id}))
        .await
        .map_err(|error| error.to_string())?;
    Ok(pane.pane_id)
}

fn owner_information(owner: &OwnerRecord) -> String {
    let mut fields = vec![
        format!("pid={}", owner.pid),
        format!("started_at_ms={}", owner.started_at_ms),
    ];
    if let Some(terminal_id) = &owner.terminal_id {
        fields.push(format!("terminal_id={terminal_id}"));
    }
    if let Some(pane_id) = &owner.pane_id {
        fields.push(format!("pane_id={pane_id}"));
    }
    format!("owner: {}", fields.join(" "))
}

fn resolve_session(cli: &Cli) -> Result<ResolvedSession, SessionKeyError> {
    let environment = env::var("HERDR_SESSION").ok();
    let managed_pane = env::var("HERDR_ENV").is_ok_and(|value| value == "1");
    session_key::resolve(cli.session.as_deref(), environment.as_deref(), managed_pane)
}

fn herdr_socket(cli: &Cli) -> Result<PathBuf, MainError> {
    if let Some(socket) = &cli.socket {
        return Ok(socket.clone());
    }
    env::var_os("HERDR_SOCKET_PATH")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or(MainError::MissingSocket)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BreadcrumbLaunchStatus {
    NotPlugin,
    Published,
    Failed(BreadcrumbPublishError),
}

fn plugin_state_dir_for_command<'a>(
    command: Option<&Command>,
    configured: Option<&'a OsStr>,
) -> Option<&'a OsStr> {
    if command.is_some() {
        return None;
    }
    configured.filter(|path| !path.is_empty())
}

fn acquire_monitor_lock_with_plugin_dir(
    root: &StateRoot,
    plugin_state_dir: Option<&OsStr>,
) -> Result<(Option<lockfile::OwnerLock>, BreadcrumbLaunchStatus), LockError> {
    let breadcrumb = match plugin_state_dir {
        Some(plugin_state_dir) => match local::publish_plugin_breadcrumb(plugin_state_dir, root) {
            Ok(()) => BreadcrumbLaunchStatus::Published,
            Err(error) => BreadcrumbLaunchStatus::Failed(error),
        },
        None => BreadcrumbLaunchStatus::NotPlugin,
    };
    let lock = lockfile::try_acquire(root)?;
    Ok((lock, breadcrumb))
}

async fn run_doctor(cli: &Cli, args: &DoctorArgs) -> ExitCode {
    let runner = DoctorVersionRunner::from_environment();
    let report =
        doctor::collect_report(cli.session.as_deref(), cli.socket.as_deref(), &runner).await;
    if args.json {
        println!("{}", doctor::render_json(&report));
    } else {
        print!("{}", doctor::render_human(&report));
    }
    ExitCode::from(report.exit_code())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn i4_local_plugin_owner_and_held_launch_publish_breadcrumb() {
        let directory = tempfile::tempdir().unwrap();
        let plugin = directory.path().join("plugin-state");
        std::fs::create_dir(&plugin).unwrap();
        std::fs::set_permissions(&plugin, Permissions::from_mode(0o700)).unwrap();
        let key = session_key::encode("plugin launch").unwrap();
        let root = lockfile::state_root_in(directory.path(), &key).unwrap();

        let (owner, first_status) =
            acquire_monitor_lock_with_plugin_dir(&root, Some(plugin.as_os_str())).unwrap();
        let owner = owner.expect("first launch should own the lock");
        assert_eq!(first_status, BreadcrumbLaunchStatus::Published);
        let breadcrumb = plugin.join("state-root.txt");
        assert_eq!(
            std::fs::read(&breadcrumb).unwrap(),
            format!("{}\n", root.0.display()).as_bytes()
        );

        std::fs::remove_file(&breadcrumb).unwrap();
        let (held, second_status) =
            acquire_monitor_lock_with_plugin_dir(&root, Some(plugin.as_os_str())).unwrap();
        assert!(held.is_none());
        assert_eq!(second_status, BreadcrumbLaunchStatus::Published);
        assert!(breadcrumb.exists());
        drop(owner);
    }

    #[test]
    fn i4_local_doctor_emit_and_nonplugin_launch_never_publish_breadcrumb() {
        let directory = tempfile::tempdir().unwrap();
        let plugin = directory.path().join("plugin-state");
        std::fs::create_dir(&plugin).unwrap();
        let key = session_key::encode("command gating").unwrap();
        let root = lockfile::state_root_in(directory.path(), &key).unwrap();
        let emit = Command::Emit(Box::new(EmitArgs {
            strict: false,
            schema_version: 1,
            event_id: "event".to_owned(),
            emitted_at_ms: 1,
            source: "controller".to_owned(),
            event_type: "task_started".to_owned(),
            task_run_id: "run".to_owned(),
            parent_task_run_id: None,
            depends_on_id: None,
            label: None,
            reason: None,
            progress: None,
            provider: None,
            native_session_id: None,
            terminal_id: None,
        }));

        let doctor = Command::Doctor(DoctorArgs { json: false });
        for command in [Some(&doctor), Some(&emit)] {
            assert!(plugin_state_dir_for_command(command, Some(plugin.as_os_str())).is_none());
        }
        let (owner, status) = acquire_monitor_lock_with_plugin_dir(&root, None).unwrap();
        assert!(owner.is_some());
        assert_eq!(status, BreadcrumbLaunchStatus::NotPlugin);
        assert!(!plugin.join("state-root.txt").exists());
    }

    #[test]
    fn i4_local_breadcrumb_failure_is_safe_and_nonfatal() {
        let directory = tempfile::tempdir().unwrap();
        let private = "PRIVATE_PLUGIN_PATH_D4A6";
        let missing_plugin = directory.path().join(private);
        let key = session_key::encode("failure launch").unwrap();
        let root = lockfile::state_root_in(directory.path(), &key).unwrap();

        let (owner, status) =
            acquire_monitor_lock_with_plugin_dir(&root, Some(missing_plugin.as_os_str())).unwrap();
        assert!(
            owner.is_some(),
            "breadcrumb failure must not block ownership"
        );
        let BreadcrumbLaunchStatus::Failed(error) = status else {
            panic!("missing plugin directory should fail publication");
        };
        assert_eq!(error.to_string(), "breadcrumb_publish_failed");
        assert!(!format!("{error:?}").contains(private));
        assert_eq!(
            plugin_state_dir_for_command(None, Some(OsStr::new(""))),
            None
        );
    }

    #[test]
    fn tracing_file_is_private_warn_filtered_and_content_free_for_malformed_records() {
        let directory = tempfile::tempdir().expect("temporary state root should exist");
        let root = StateRoot(directory.path().to_path_buf());
        let (subscriber, occurrence_sink) =
            build_tracing_subscriber(&root).expect("subscriber should build");
        let raw_sentinel = "MALFORMED_RAW_SENTINEL_I2E_8D97";

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(secret = raw_sentinel, "filtered informational event");
            tracing::warn!(
                warning_code = "provider_record_malformed",
                provider = "codex",
                byte_offset = 41_u64,
                error_code = "codex_json",
                "malformed provider record"
            );
        });
        occurrence_sink
            .append(b"HERDR_TOP_PERSISTENCE_V1 {\"schema_version\":1}\n")
            .expect("occurrence append should share and flush the log handle");

        let path = tracing_log_path(&root);
        let metadata = std::fs::metadata(&path).expect("log metadata should read");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let contents = std::fs::read_to_string(path).expect("log should be UTF-8");
        assert!(contents.contains("malformed provider record"));
        assert!(contents.contains("codex_json"));
        assert!(contents.contains("HERDR_TOP_PERSISTENCE_V1"));
        assert!(!contents.contains(raw_sentinel));
    }
}
