#![deny(unsafe_code)]

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use herdr_top::herdr::collector::{self, CollectorError};
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

#[derive(Debug, Parser)]
#[command(name = "herdr-top")]
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
    Doctor,
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
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        None => run_monitor(&cli).await,
        Some(Command::Emit(args)) => return run_emit(&cli, args).await,
        Some(Command::Doctor) => run_doctor(&cli),
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

async fn run_monitor(cli: &Cli) -> Result<(), MainError> {
    let resolved = resolve_session(cli)?;
    let root = lockfile::state_root(resolved.session_key())?;
    let owner_lock = lockfile::try_acquire(&root)?;

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

    let _schema = store::preflight_schema(&root)?;
    let store = store::open_writer(&root)?;
    let restored = store.load_restored_state()?;
    let (lifecycle, writer) = store::spawn_writer(store)?;
    let session_name = resolved.session_key().name().to_owned();
    let collector = match collector::spawn_with_controller(
        socket,
        session_name.clone(),
        restored,
        writer,
        controller_listener,
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
            source_coverage: format!("herdr; controller={controller_status:?}"),
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

fn run_doctor(cli: &Cli) -> Result<(), MainError> {
    let resolved = resolve_session(cli)?;
    let root = lockfile::state_root(resolved.session_key())?;
    println!("resolver_source: {}", resolved.source());
    println!("state_root: {}", root.0.display());
    println!("controller_socket: implemented");
    Ok(())
}
