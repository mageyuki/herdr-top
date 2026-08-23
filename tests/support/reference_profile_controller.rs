use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const SOURCE_FIXTURE_BODY: &str = r#"set -euo pipefail
case $- in *p*) ;; *) exit 20 ;; esac
runner_script=$1
shift
readonly runner_script
source "$runner_script"
fixture_operation=$1
shift
readonly fixture_operation
case "$fixture_operation" in
  orchestration) run_orchestration_fixture "$@" ;;
  output-containment) run_output_containment_fixture "$@" ;;
  *) exit 20 ;;
esac"#;

const SOURCE_FIXTURE_ROLES: [&str; 16] = [
    "env",
    "id",
    "mkdir",
    "mktemp",
    "mv",
    "pidstat",
    "prlimit",
    "readlink",
    "rmdir",
    "setsid",
    "sha256sum",
    "sleep",
    "stat",
    "taskset",
    "time",
    "unlink",
];

const CALLER_KEYS: [&str; 7] = [
    "CARGO_HOME",
    "HERDR_INCREMENT5_ATTEMPT_ID",
    "HOME",
    "LC_ALL",
    "PATH",
    "RUSTUP_HOME",
    "TZ",
];

const SOURCE_CALLER_KEYS: [&str; 8] = [
    "CARGO_HOME",
    "HERDR_INCREMENT5_ATTEMPT_ID",
    "HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY",
    "HOME",
    "LC_ALL",
    "PATH",
    "RUSTUP_HOME",
    "TZ",
];

#[derive(Clone, Debug)]
struct IdentityArgument {
    requested: PathBuf,
    canonical: PathBuf,
    sha256: String,
}

#[derive(Clone, Debug)]
struct DerivedIdentity {
    requested: PathBuf,
    canonical: PathBuf,
    sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LaunchMode {
    Launch,
    Runner,
    SourceFixture,
}

fn developer_home_text(developer_home: &Path) -> Result<&str, &'static str> {
    developer_home
        .to_str()
        .ok_or("developer home was not valid UTF-8")
}

fn developer_home_path(developer_home: &Path, relative: &str) -> Result<String, &'static str> {
    developer_home
        .join(relative)
        .into_os_string()
        .into_string()
        .map_err(|_| "developer home path was not valid UTF-8")
}

fn authoritative_requested_paths(developer_home: &Path) -> Result<Vec<String>, &'static str> {
    let mut paths = vec![developer_home_path(developer_home, ".cargo/bin/rustup")?];
    paths.extend(
        [
            "/usr/bin/awk",
            "/usr/bin/bash",
            "/usr/bin/env",
            "/usr/bin/findmnt",
            "/usr/bin/git",
            "/usr/bin/id",
            "/usr/bin/jq",
            "/usr/bin/lsblk",
            "/usr/bin/lscpu",
            "/usr/bin/mkdir",
            "/usr/bin/mktemp",
            "/usr/bin/mv",
            "/usr/bin/pidstat",
            "/usr/bin/prlimit",
            "/usr/bin/readlink",
            "/usr/bin/rg",
            "/usr/bin/rmdir",
            "/usr/bin/setsid",
            "/usr/bin/sha256sum",
            "/usr/bin/sleep",
            "/usr/bin/stat",
            "/usr/bin/taskset",
            "/usr/bin/time",
            "/usr/bin/uname",
            "/usr/bin/unlink",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    Ok(paths)
}

#[derive(Debug)]
struct LaunchRequest {
    developer_home: PathBuf,
    self_identity: IdentityArgument,
    mode: LaunchMode,
    runner_identity: Option<IdentityArgument>,
    fixture_tools: Vec<(String, PathBuf)>,
    program_identity: IdentityArgument,
    environment: BTreeMap<String, String>,
    argv: Vec<String>,
}

#[derive(Debug)]
struct Cursor {
    values: Vec<String>,
    index: usize,
}

impl Cursor {
    fn new(values: Vec<String>) -> Self {
        Self { values, index: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.values.get(self.index).map(String::as_str)
    }

    fn take(&mut self) -> Result<String, &'static str> {
        let value = self
            .values
            .get(self.index)
            .cloned()
            .ok_or("argument was missing")?;
        self.index += 1;
        Ok(value)
    }

    fn expect(&mut self, expected: &'static str) -> Result<(), &'static str> {
        if self.take()?.as_str() == expected {
            Ok(())
        } else {
            Err("closed grammar was violated")
        }
    }

    fn finish(self) -> Result<(), &'static str> {
        if self.index == self.values.len() {
            Ok(())
        } else {
            Err("unexpected arguments remained")
        }
    }
}

fn main() {
    let status = match run() {
        Ok(code @ (0 | 10 | 20)) => code,
        Ok(_) => 20,
        Err(error) => {
            eprintln!("error: {error}");
            20
        }
    };
    std::process::exit(status);
}

fn run() -> Result<i32, &'static str> {
    if env::vars_os().next().is_some() {
        return Err("native Controller environment was not empty");
    }
    let arguments = env::args_os()
        .skip(1)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "argument was not valid UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request = parse_request(arguments)?;
    execute(request)
}

fn parse_request(arguments: Vec<String>) -> Result<LaunchRequest, &'static str> {
    let mut cursor = Cursor::new(arguments);
    cursor.expect("--developer-home")?;
    let developer_home = PathBuf::from(cursor.take()?);
    let self_identity = parse_identity(&mut cursor, "self")?;
    let mode = match cursor.take()?.as_str() {
        "launch" => LaunchMode::Launch,
        "launch-runner" => LaunchMode::Runner,
        "launch-runner-source-fixture" => LaunchMode::SourceFixture,
        _ => return Err("launch mode was invalid"),
    };
    let runner_identity = if mode == LaunchMode::Launch {
        None
    } else {
        Some(parse_identity(&mut cursor, "runner-script")?)
    };
    let mut fixture_tools = Vec::new();
    while cursor.peek() == Some("--fixture-tool") {
        cursor.expect("--fixture-tool")?;
        let value = cursor.take()?;
        let (role, path) = value.split_once('=').ok_or("fixture tool was malformed")?;
        if role.is_empty() || path.is_empty() || path.contains('=') {
            return Err("fixture tool was malformed");
        }
        fixture_tools.push((role.to_owned(), PathBuf::from(path)));
    }
    let program_identity = parse_identity(&mut cursor, "program")?;
    let mut environment = BTreeMap::new();
    while cursor.peek() == Some("--env") {
        cursor.expect("--env")?;
        let value = cursor.take()?;
        let (key, value) = value.split_once('=').ok_or("environment was malformed")?;
        if !valid_environment_key(key)
            || environment
                .insert(key.to_owned(), value.to_owned())
                .is_some()
        {
            return Err("environment key was invalid or duplicated");
        }
    }
    cursor.expect("--")?;
    let mut argv = Vec::new();
    while cursor.peek().is_some() {
        argv.push(cursor.take()?);
    }
    cursor.finish()?;
    Ok(LaunchRequest {
        developer_home,
        self_identity,
        mode,
        runner_identity,
        fixture_tools,
        program_identity,
        environment,
        argv,
    })
}

fn parse_identity(
    cursor: &mut Cursor,
    prefix: &'static str,
) -> Result<IdentityArgument, &'static str> {
    let requested_flag = format!("--{prefix}-requested");
    let canonical_flag = format!("--{prefix}-canonical");
    let digest_flag = format!("--{prefix}-sha256");
    if cursor.take()? != requested_flag {
        return Err("requested identity flag was invalid");
    }
    let requested = PathBuf::from(cursor.take()?);
    if cursor.take()? != canonical_flag {
        return Err("canonical identity flag was invalid");
    }
    let canonical = PathBuf::from(cursor.take()?);
    if cursor.take()? != digest_flag {
        return Err("digest identity flag was invalid");
    }
    let sha256 = cursor.take()?;
    Ok(IdentityArgument {
        requested,
        canonical,
        sha256,
    })
}

fn execute(mut request: LaunchRequest) -> Result<i32, &'static str> {
    validate_developer_home(&request.developer_home)?;
    let self_derived = validate_identity(&request.self_identity)?;
    let current = fs::canonicalize(env::current_exe().map_err(|_| "current executable failed")?)
        .map_err(|_| "current executable canonicalization failed")?;
    if self_derived.canonical != current {
        return Err("self identity disagreed with current executable");
    }
    let program = validate_identity(&request.program_identity)?;
    reject_reserved_environment(&request.environment)?;

    match request.mode {
        LaunchMode::Launch => {
            if request.runner_identity.is_some() || !request.fixture_tools.is_empty() {
                return Err("generic launch carried runner evidence");
            }
        }
        LaunchMode::Runner | LaunchMode::SourceFixture => {
            let runner_argument = request
                .runner_identity
                .as_ref()
                .ok_or("runner identity was missing")?;
            let runner = validate_identity(runner_argument)?;
            validate_bash_program(&request.program_identity, &program)?;
            validate_runner_environment(
                request.mode,
                &request.environment,
                &request.developer_home,
            )?;
            let manifest = match request.mode {
                LaunchMode::Runner => {
                    if !request.fixture_tools.is_empty() {
                        return Err("authoritative launch carried fixture tools");
                    }
                    validate_runner_argv(&request.argv, &runner.canonical)?;
                    serialize_authoritative_manifest(&request.developer_home)?
                }
                LaunchMode::SourceFixture => {
                    validate_source_fixture_argv(&request.argv, &runner.canonical)?;
                    serialize_source_fixture_manifest(
                        &request.fixture_tools,
                        &request.developer_home,
                    )?
                }
                LaunchMode::Launch => unreachable!(),
            };
            inject_runner_evidence(
                &mut request.environment,
                &request.self_identity,
                runner_argument,
                request.mode,
                manifest,
            )?;
        }
    }

    let status = Command::new(&program.canonical)
        .env_clear()
        .envs(&request.environment)
        .args(&request.argv)
        .status()
        .map_err(|_| "child launch failed")?;
    match status.code() {
        Some(code @ (0 | 10 | 20)) => Ok(code),
        _ => Err("child status was outside the closed set"),
    }
}

fn validate_identity(argument: &IdentityArgument) -> Result<DerivedIdentity, &'static str> {
    validate_path_text(&argument.requested)?;
    validate_path_text(&argument.canonical)?;
    if !argument.requested.is_absolute()
        || !argument.canonical.is_absolute()
        || !is_lower_hex(&argument.sha256, 64)
    {
        return Err("identity shape was invalid");
    }
    let canonical = fs::canonicalize(&argument.requested)
        .map_err(|_| "requested path could not be canonicalized")?;
    if canonical != argument.canonical
        || fs::canonicalize(&argument.canonical)
            .map_err(|_| "canonical path could not be canonicalized")?
            != argument.canonical
    {
        return Err("requested and canonical paths disagreed");
    }
    let metadata = fs::metadata(&canonical).map_err(|_| "executable metadata failed")?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err("identity target was not a regular executable");
    }
    let sha256 = sha256_path(&canonical).map_err(|_| "executable hashing failed")?;
    if sha256 != argument.sha256 {
        return Err("identity digest disagreed");
    }
    Ok(DerivedIdentity {
        requested: argument.requested.clone(),
        canonical,
        sha256,
    })
}

fn validate_developer_home(path: &Path) -> Result<(), &'static str> {
    validate_path_text(path)?;
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err("developer home was not a normalized absolute path");
    }
    Ok(())
}

fn derive_identity(requested: &Path) -> Result<DerivedIdentity, &'static str> {
    validate_path_text(requested)?;
    if !requested.is_absolute() {
        return Err("requested tool path was not absolute");
    }
    let canonical = fs::canonicalize(requested).map_err(|_| "tool canonicalization failed")?;
    validate_path_text(&canonical)?;
    let metadata = fs::metadata(&canonical).map_err(|_| "tool metadata failed")?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err("tool was not a regular executable");
    }
    let sha256 = sha256_path(&canonical).map_err(|_| "tool hashing failed")?;
    Ok(DerivedIdentity {
        requested: requested.to_path_buf(),
        canonical,
        sha256,
    })
}

fn sha256_path(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_bash_program(
    argument: &IdentityArgument,
    derived: &DerivedIdentity,
) -> Result<(), &'static str> {
    let expected = derive_identity(Path::new("/usr/bin/bash"))?;
    if argument.requested != Path::new("/usr/bin/bash")
        || derived.canonical != expected.canonical
        || derived.sha256 != expected.sha256
    {
        return Err("runner program was not the frozen Bash entry");
    }
    Ok(())
}

fn validate_runner_argv(argv: &[String], runner: &Path) -> Result<(), &'static str> {
    if argv.len() < 2 || argv[0] != "-p" || Path::new(&argv[1]) != runner {
        return Err("authoritative runner argv prefix was invalid");
    }
    Ok(())
}

fn validate_source_fixture_argv(argv: &[String], runner: &Path) -> Result<(), &'static str> {
    if argv.len() < 7
        || argv[0] != "-p"
        || argv[1] != "-c"
        || argv[2] != SOURCE_FIXTURE_BODY
        || argv[3] != "herdr-i5-source-fixture"
        || Path::new(&argv[4]) != runner
    {
        return Err("source fixture argv prefix was invalid");
    }
    match argv[5].as_str() {
        "orchestration" => {
            if argv.len() < 7 {
                return Err("orchestration fixture operands were missing");
            }
        }
        "output-containment" => validate_containment_operands(&argv[6..])?,
        _ => return Err("source fixture operation was invalid"),
    }
    Ok(())
}

fn validate_containment_operands(values: &[String]) -> Result<(), &'static str> {
    if values.len() < 3 {
        return Err("containment operands were missing");
    }
    let repository = Path::new(&values[0]);
    let count = parse_canonical_usize(&values[1])?;
    if values.len() != count + 3 {
        return Err("worktree root count disagreed");
    }
    validate_canonical_root(repository)?;
    for value in &values[2..2 + count] {
        validate_canonical_root(Path::new(value))?;
    }
    let output = Path::new(&values[2 + count]);
    validate_path_text(output)?;
    if !output.is_absolute() {
        return Err("output path was not absolute");
    }
    Ok(())
}

fn validate_canonical_root(path: &Path) -> Result<(), &'static str> {
    validate_path_text(path)?;
    if !path.is_absolute()
        || fs::canonicalize(path).map_err(|_| "root canonicalization failed")? != path
    {
        return Err("root was not a canonical absolute path");
    }
    Ok(())
}

fn serialize_source_fixture_manifest(
    tools: &[(String, PathBuf)],
    developer_home: &Path,
) -> Result<String, &'static str> {
    if tools.len() != SOURCE_FIXTURE_ROLES.len() {
        return Err("source fixture role count was invalid");
    }
    let mut identities = Vec::with_capacity(tools.len());
    for ((role, requested), expected_role) in tools.iter().zip(SOURCE_FIXTURE_ROLES) {
        if role != expected_role {
            return Err("source fixture role ordering was invalid");
        }
        if requested.starts_with(developer_home) {
            return Err("source fixture tool used a workstation path");
        }
        if requested.file_name() != Some(std::ffi::OsStr::new(expected_role)) {
            return Err("source fixture tool basename disagreed with role");
        }
        let identity = derive_identity(requested)?;
        if identity.canonical.starts_with(developer_home) {
            return Err("source fixture tool used a workstation path");
        }
        identities.push(identity);
    }
    serialize_manifest(&identities)
}

fn serialize_authoritative_manifest(developer_home: &Path) -> Result<String, &'static str> {
    let requested_paths = authoritative_requested_paths(developer_home)?;
    let mut identities = Vec::with_capacity(requested_paths.len());
    for requested in requested_paths {
        let identity = derive_identity(Path::new(&requested))?;
        if requested.ends_with("/rustup")
            && identity
                .canonical
                .components()
                .any(|component| component.as_os_str() == "mise")
        {
            return Err("rustup canonical path used mise");
        }
        identities.push(identity);
    }
    serialize_manifest(&identities)
}

fn serialize_manifest(identities: &[DerivedIdentity]) -> Result<String, &'static str> {
    let mut value = String::new();
    for identity in identities {
        let requested = identity
            .requested
            .to_str()
            .ok_or("requested path was not UTF-8")?;
        let canonical = identity
            .canonical
            .to_str()
            .ok_or("canonical path was not UTF-8")?;
        if requested.contains(['\t', '\n', '\r']) || canonical.contains(['\t', '\n', '\r']) {
            return Err("manifest path contained a delimiter");
        }
        value.push_str(requested);
        value.push('\t');
        value.push_str(canonical);
        value.push('\t');
        value.push_str(&identity.sha256);
        value.push('\n');
    }
    Ok(value)
}

fn validate_runner_environment(
    mode: LaunchMode,
    environment: &BTreeMap<String, String>,
    developer_home: &Path,
) -> Result<(), &'static str> {
    let expected_keys = match mode {
        LaunchMode::Runner => CALLER_KEYS.as_slice(),
        LaunchMode::SourceFixture => SOURCE_CALLER_KEYS.as_slice(),
        LaunchMode::Launch => return Err("generic launch used runner environment validation"),
    };
    if environment.keys().map(String::as_str).collect::<Vec<_>>() != expected_keys {
        return Err("runner caller environment keys were invalid");
    }
    let expected_values = BTreeMap::from([
        ("CARGO_HOME", developer_home_path(developer_home, ".cargo")?),
        ("HOME", developer_home_text(developer_home)?.to_owned()),
        ("LC_ALL", "C".to_owned()),
        ("PATH", "/usr/bin:/bin".to_owned()),
        (
            "RUSTUP_HOME",
            developer_home_path(developer_home, ".rustup")?,
        ),
        ("TZ", "UTC".to_owned()),
    ]);
    for (key, expected) in expected_values {
        if environment.get(key).map(String::as_str) != Some(expected.as_str()) {
            return Err("runner caller environment value was invalid");
        }
    }
    let attempt = environment
        .get("HERDR_INCREMENT5_ATTEMPT_ID")
        .ok_or("attempt ID was missing")?;
    if !valid_attempt_id(attempt) {
        return Err("attempt ID was invalid");
    }
    if mode == LaunchMode::SourceFixture
        && environment
            .get("HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY")
            .map(String::as_str)
            != Some("1")
    {
        return Err("source fixture library guard was invalid");
    }
    Ok(())
}

fn inject_runner_evidence(
    environment: &mut BTreeMap<String, String>,
    controller: &IdentityArgument,
    runner: &IdentityArgument,
    mode: LaunchMode,
    manifest: String,
) -> Result<(), &'static str> {
    let entries = [
        (
            "HERDR_INCREMENT5_CONTROLLER_REQUESTED",
            controller
                .requested
                .to_str()
                .ok_or("controller path was not UTF-8")?,
        ),
        (
            "HERDR_INCREMENT5_CONTROLLER_CANONICAL",
            controller
                .canonical
                .to_str()
                .ok_or("controller path was not UTF-8")?,
        ),
        ("HERDR_INCREMENT5_CONTROLLER_SHA256", &controller.sha256),
        (
            "HERDR_INCREMENT5_RUNNER_REQUESTED",
            runner
                .requested
                .to_str()
                .ok_or("runner path was not UTF-8")?,
        ),
        (
            "HERDR_INCREMENT5_RUNNER_CANONICAL",
            runner
                .canonical
                .to_str()
                .ok_or("runner path was not UTF-8")?,
        ),
        ("HERDR_INCREMENT5_RUNNER_SHA256", &runner.sha256),
    ];
    for (key, value) in entries {
        if environment
            .insert(key.to_owned(), value.to_owned())
            .is_some()
        {
            return Err("reserved environment was already present");
        }
    }
    let manifest_key = match mode {
        LaunchMode::Runner => "HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1",
        LaunchMode::SourceFixture => "HERDR_INCREMENT5_BOOTSTRAP_TOOLS_SOURCE_FIXTURE_V1",
        LaunchMode::Launch => return Err("generic launch injected runner evidence"),
    };
    if environment
        .insert(manifest_key.to_owned(), manifest)
        .is_some()
    {
        return Err("manifest environment was already present");
    }
    Ok(())
}

fn reject_reserved_environment(environment: &BTreeMap<String, String>) -> Result<(), &'static str> {
    if environment.keys().any(|key| {
        key.starts_with("HERDR_INCREMENT5_CONTROLLER_")
            || key.starts_with("HERDR_INCREMENT5_RUNNER_")
            || key.starts_with("HERDR_INCREMENT5_BOOTSTRAP_")
    }) {
        return Err("caller supplied reserved environment");
    }
    Ok(())
}

fn valid_environment_key(key: &str) -> bool {
    let mut bytes = key.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_attempt_id(value: &str) -> bool {
    value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()) && value != "00000000"
}

fn parse_canonical_usize(value: &str) -> Result<usize, &'static str> {
    if value.is_empty()
        || value != "0" && value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("count was not canonical");
    }
    value.parse().map_err(|_| "count overflowed")
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_path_text(path: &Path) -> Result<(), &'static str> {
    let value = path.to_str().ok_or("path was not valid UTF-8")?;
    if value.contains(['\t', '\n', '\r', '\0']) {
        Err("path contained a forbidden byte")
    } else {
        Ok(())
    }
}
