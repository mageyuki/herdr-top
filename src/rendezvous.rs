#![allow(unsafe_code)]
//! T4 validated runtime-directory handle, sentinel, and controller-socket pre-bind gate.

use std::env;
#[cfg(target_os = "macos")]
use std::ffi::OsString;
use std::ffi::{CStr, CString, OsStr};
use std::fs::{File, Permissions};
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use thiserror::Error;
use ulid::Ulid;

use crate::lockfile::OwnerLock;
use crate::session_key::SessionKey;

const DIRECTORY_MODE: libc::mode_t = 0o700;
const FILE_MODE: u32 = 0o600;
const RUNTIME_CHILD: &str = "herdr-top";

#[cfg(target_os = "linux")]
const SOCKET_PATH_LIMIT: usize = 108;
#[cfg(target_os = "macos")]
const SOCKET_PATH_LIMIT: usize = 104;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("rendezvous socket-path limits are defined only for Linux and macOS");

/// A validated handle to the private `herdr-top` runtime directory.
#[derive(Debug)]
pub struct ValidatedRuntimeDir(OwnedFd);

/// The vertical slice's controller-socket readiness result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerSocketStatus {
    /// The path passed every pre-bind check; binding lands after this vertical slice.
    NotImplementedInVerticalSlice,
    /// Controller-event input is unavailable, but the owner may continue starting.
    Unavailable(ControllerInputUnavailable),
}

/// A persistent diagnostic reason why Controller-event input is unavailable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerInputUnavailable {
    /// A socket exists without the sentinel needed to establish its identity.
    UnsafeOrphan,
    /// The sentinel identifies a different exact session name.
    Collision {
        /// The name found in the existing sentinel.
        found: String,
    },
    /// A live endpoint answered even though this owner holds the state lock.
    LiveEndpointUnderLock,
    /// A later slice attempted and failed to bind the controller socket.
    BindFailure(String),
    /// The full socket pathname, including its NUL terminator, does not fit.
    PathTooLong,
}

/// Errors from runtime-directory validation and the socket pre-bind gate.
#[derive(Debug, Error)]
pub enum RvError {
    /// A runtime directory is not a real, private directory owned by this process.
    #[error("hostile runtime directory at {path:?}: {reason}")]
    HostileDir {
        /// The directory path that failed validation.
        path: PathBuf,
        /// The failed ownership, type, mode, or no-follow condition.
        reason: String,
    },
    /// The full socket path cannot fit in the platform's `sockaddr_un::sun_path`.
    #[error(
        "controller socket path {path:?} requires {length} bytes including NUL; platform limit is {limit}"
    )]
    PathTooLong {
        /// The full socket path.
        path: PathBuf,
        /// Required bytes, including the terminating NUL.
        length: usize,
        /// Platform `sun_path` capacity.
        limit: usize,
    },
    /// A non-socket occupies the controller socket's child name.
    #[error("controller socket path {path:?} is occupied by a non-socket entry (mode {mode:#o})")]
    NonSocketEntry {
        /// The occupied socket path.
        path: PathBuf,
        /// The entry's complete mode from `fstatat`.
        mode: libc::mode_t,
    },
    /// A live-endpoint probe failed without proving that the socket is stale.
    #[error("cannot safely probe controller socket at {path:?}: {source}")]
    SocketProbe {
        /// The socket path being probed.
        path: PathBuf,
        /// The conservative probe failure.
        #[source]
        source: io::Error,
    },
    /// A runtime filesystem operation failed.
    #[error("{operation} failed at {path:?}: {source}")]
    Io {
        /// The operation being attempted.
        operation: &'static str,
        /// The path involved in the operation.
        path: PathBuf,
        /// The underlying operating-system error.
        #[source]
        source: io::Error,
    },
}

/// Resolves the environment-independent runtime base.
///
/// A non-empty XDG runtime directory wins. Otherwise the base is a private,
/// UID-specific child beneath non-empty `tmpdir`, or beneath `/tmp`.
pub fn runtime_base(xdg: Option<&OsStr>, tmpdir: Option<&OsStr>, uid: u32) -> PathBuf {
    if let Some(xdg) = xdg.filter(|value| !value.is_empty()) {
        return PathBuf::from(xdg);
    }

    let temporary = tmpdir
        .filter(|value| !value.is_empty())
        .map_or_else(|| PathBuf::from("/tmp"), PathBuf::from);
    temporary.join(format!("herdr-top-{uid}"))
}

/// Opens the process runtime directory using `XDG_RUNTIME_DIR`/`TMPDIR`.
///
/// This is the only environment-reading entry point in this module.
pub fn open_runtime_dir() -> Result<ValidatedRuntimeDir, RvError> {
    let uid = effective_uid();
    let xdg = env::var_os("XDG_RUNTIME_DIR");
    let tmpdir = env::var_os("TMPDIR");
    let base = runtime_base(xdg.as_deref(), tmpdir.as_deref(), uid);
    open_runtime_dir_at_for_uid(&base, uid)
}

/// Reads the kernel hostname through the platform libc boundary.
#[must_use]
pub fn gethostname() -> Option<String> {
    let mut buffer = [0_u8; 256];
    // SAFETY: `buffer` is writable for exactly `buffer.len()` bytes and remains alive for the
    // call. The return code is checked before reading, and the initialized zero-filled buffer
    // bounds the search even on platforms that truncate without a trailing NUL.
    let result =
        unsafe { libc::gethostname(buffer.as_mut_ptr().cast::<libc::c_char>(), buffer.len()) };
    if result != 0 {
        return None;
    }
    let length = buffer
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(buffer.len());
    if length == 0 {
        return None;
    }
    Some(String::from_utf8_lossy(&buffer[..length]).into_owned())
}

/// Creates or opens and validates `base` and its `herdr-top` child.
///
/// Neither level follows a symlink, and no pre-existing entry is chmodded.
pub fn open_runtime_dir_at(base: &Path) -> Result<ValidatedRuntimeDir, RvError> {
    open_runtime_dir_at_for_uid(base, effective_uid())
}

fn open_runtime_dir_at_for_uid(base: &Path, uid: u32) -> Result<ValidatedRuntimeDir, RvError> {
    create_directory_path(base)?;
    let base_fd = open_directory_path(base)?;
    validate_directory_fd(base, &base_fd, uid)?;

    let child_name = component_cstring(OsStr::new(RUNTIME_CHILD)).map_err(|source| {
        io_error(
            "encode runtime child name",
            base.join(RUNTIME_CHILD),
            source,
        )
    })?;
    let child_path = base.join(RUNTIME_CHILD);
    create_directory_at(base_fd.as_raw_fd(), &child_name, &child_path)?;
    let child_fd = open_directory_at(base_fd.as_raw_fd(), &child_name, &child_path)?;
    validate_directory_fd(&child_path, &child_fd, uid)?;

    Ok(ValidatedRuntimeDir(child_fd))
}

/// Checks that the full Unix-socket path and its NUL terminator fit `sun_path`.
pub fn preflight_len(sock: &Path) -> Result<(), RvError> {
    let length = sock.as_os_str().as_bytes().len().saturating_add(1);
    if length > SOCKET_PATH_LIMIT {
        Err(RvError::PathTooLong {
            path: sock.to_path_buf(),
            length,
            limit: SOCKET_PATH_LIMIT,
        })
    } else {
        Ok(())
    }
}

/// Runs the sentinel publication and stale-socket pre-bind state machine.
///
/// The `OwnerLock` reference is the witness that this process holds the exact
/// session's state lock. This vertical slice deliberately stops before bind.
pub fn prepare_controller_socket(
    dir: &ValidatedRuntimeDir,
    key: &SessionKey,
    _lock: &OwnerLock,
) -> Result<ControllerSocketStatus, RvError> {
    let directory_path = runtime_directory_path(dir)?;
    let socket_name = format!("{}.sock", key.hash16());
    let sentinel_name = format!("{}.name", key.hash16());
    let socket_path = directory_path.join(&socket_name);

    match preflight_len(&socket_path) {
        Ok(()) => {}
        Err(RvError::PathTooLong { .. }) => {
            return Ok(ControllerSocketStatus::Unavailable(
                ControllerInputUnavailable::PathTooLong,
            ));
        }
        Err(source) => return Err(source),
    }

    let socket_stat = stat_child(dir, OsStr::new(&socket_name), &socket_path)?;
    let sentinel_path = directory_path.join(&sentinel_name);
    let sentinel_stat = stat_child(dir, OsStr::new(&sentinel_name), &sentinel_path)?;
    if socket_stat.is_some() && sentinel_stat.is_none() {
        return Ok(ControllerSocketStatus::Unavailable(
            ControllerInputUnavailable::UnsafeOrphan,
        ));
    }

    if let Some(found) = publish_sentinel(dir, key, &sentinel_name, &sentinel_path)? {
        return Ok(ControllerSocketStatus::Unavailable(
            ControllerInputUnavailable::Collision { found },
        ));
    }

    if stat_child(dir, OsStr::new(&socket_name), &socket_path)?.is_some() {
        match UnixStream::connect(&socket_path) {
            Ok(_connection) => {
                return Ok(ControllerSocketStatus::Unavailable(
                    ControllerInputUnavailable::LiveEndpointUnderLock,
                ));
            }
            Err(source) if source.raw_os_error() == Some(libc::ENOENT) => {}
            Err(source) if source.raw_os_error() == Some(libc::ECONNREFUSED) => {
                remove_proven_stale_socket(dir, &socket_name, &socket_path)?;
            }
            Err(source) => {
                return Err(RvError::SocketProbe {
                    path: socket_path,
                    source,
                });
            }
        }
    }

    Ok(ControllerSocketStatus::NotImplementedInVerticalSlice)
}

/// Performs orderly shutdown for this unbound vertical slice.
///
/// No status in this slice represents a successfully bound socket, so shutdown
/// unlinks nothing. The final sentinel is retained. A later bound-owner type
/// will carry the proof required to close and unlink exactly its own socket.
pub fn shutdown_controller_socket(
    _dir: &ValidatedRuntimeDir,
    _key: &SessionKey,
    _lock: &OwnerLock,
) {
}

fn publish_sentinel(
    dir: &ValidatedRuntimeDir,
    key: &SessionKey,
    sentinel_name: &str,
    sentinel_path: &Path,
) -> Result<Option<String>, RvError> {
    let temp_name = format!("{sentinel_name}.tmp.{}.{}", std::process::id(), Ulid::new());
    let temp_path = sentinel_path.with_file_name(&temp_name);
    let temp_component = component_cstring(OsStr::new(&temp_name))
        .map_err(|source| io_error("encode sentinel temp name", temp_path.clone(), source))?;
    let sentinel_component = component_cstring(OsStr::new(sentinel_name))
        .map_err(|source| io_error("encode sentinel name", sentinel_path.to_path_buf(), source))?;
    let temp_fd = openat_owned(
        dir.0.as_raw_fd(),
        &temp_component,
        libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC,
        FILE_MODE as libc::mode_t,
    )
    .map_err(|source| io_error("create sentinel temp", temp_path.clone(), source))?;
    let mut temp_guard = TempEntry::new(dir.0.as_raw_fd(), temp_component, temp_path.clone());
    let mut temp_file = File::from(temp_fd);

    let publication = (|| -> Result<LinkOutcome, RvError> {
        temp_file
            .set_permissions(Permissions::from_mode(FILE_MODE))
            .map_err(|source| io_error("set sentinel temp mode", temp_path.clone(), source))?;
        temp_file
            .write_all(key.name().as_bytes())
            .map_err(|source| io_error("write sentinel temp", temp_path.clone(), source))?;
        temp_file
            .sync_all()
            .map_err(|source| io_error("sync sentinel temp", temp_path.clone(), source))?;

        match linkat_no_replace(dir.0.as_raw_fd(), temp_guard.name(), &sentinel_component) {
            Ok(()) => Ok(LinkOutcome::Installed),
            Err(source) if source.raw_os_error() == Some(libc::EEXIST) => {
                Ok(LinkOutcome::AlreadyExists)
            }
            Err(source) => Err(io_error(
                "install sentinel",
                sentinel_path.to_path_buf(),
                source,
            )),
        }
    })();

    temp_guard.discard()?;

    match publication? {
        LinkOutcome::Installed => Ok(None),
        LinkOutcome::AlreadyExists => {
            let found = read_child(dir, &sentinel_component, sentinel_path)?;
            if found == key.name().as_bytes() {
                Ok(None)
            } else {
                Ok(Some(String::from_utf8_lossy(&found).into_owned()))
            }
        }
    }
}

fn remove_proven_stale_socket(
    dir: &ValidatedRuntimeDir,
    socket_name: &str,
    socket_path: &Path,
) -> Result<(), RvError> {
    let Some(stat) = stat_child(dir, OsStr::new(socket_name), socket_path)? else {
        return Ok(());
    };
    if stat.st_mode & libc::S_IFMT != libc::S_IFSOCK {
        return Err(RvError::NonSocketEntry {
            path: socket_path.to_path_buf(),
            mode: stat.st_mode,
        });
    }

    let component = component_cstring(OsStr::new(socket_name))
        .map_err(|source| io_error("encode socket name", socket_path.to_path_buf(), source))?;
    match unlinkat_child(dir.0.as_raw_fd(), &component) {
        Ok(()) => Ok(()),
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => Ok(()),
        Err(source) => Err(io_error(
            "unlink proven-stale socket",
            socket_path.to_path_buf(),
            source,
        )),
    }
}

fn read_child(dir: &ValidatedRuntimeDir, name: &CStr, path: &Path) -> Result<Vec<u8>, RvError> {
    let fd = openat_owned(
        dir.0.as_raw_fd(),
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )
    .map_err(|source| io_error("open sentinel", path.to_path_buf(), source))?;
    let mut file = File::from(fd);
    let mut found = Vec::new();
    file.read_to_end(&mut found)
        .map_err(|source| io_error("read sentinel", path.to_path_buf(), source))?;
    Ok(found)
}

fn stat_child(
    dir: &ValidatedRuntimeDir,
    name: &OsStr,
    path: &Path,
) -> Result<Option<libc::stat>, RvError> {
    let component = component_cstring(name)
        .map_err(|source| io_error("encode child name", path.to_path_buf(), source))?;
    match fstatat_nofollow(dir.0.as_raw_fd(), &component) {
        Ok(stat) => Ok(Some(stat)),
        Err(source) if source.raw_os_error() == Some(libc::ENOENT) => Ok(None),
        Err(source) => Err(io_error("stat runtime child", path.to_path_buf(), source)),
    }
}

fn create_directory_path(path: &Path) -> Result<(), RvError> {
    let encoded = path_cstring(path)
        .map_err(|source| io_error("encode runtime directory", path.to_path_buf(), source))?;
    match mkdir_path(&encoded, DIRECTORY_MODE) {
        Ok(()) => Ok(()),
        Err(source) if source.raw_os_error() == Some(libc::EEXIST) => Ok(()),
        Err(source) => Err(io_error(
            "create runtime directory",
            path.to_path_buf(),
            source,
        )),
    }
}

fn create_directory_at(parent_fd: RawFd, name: &CStr, path: &Path) -> Result<(), RvError> {
    match mkdirat_child(parent_fd, name, DIRECTORY_MODE) {
        Ok(()) => Ok(()),
        Err(source) if source.raw_os_error() == Some(libc::EEXIST) => Ok(()),
        Err(source) => Err(io_error(
            "create runtime child directory",
            path.to_path_buf(),
            source,
        )),
    }
}

fn open_directory_path(path: &Path) -> Result<OwnedFd, RvError> {
    let encoded = path_cstring(path)
        .map_err(|source| io_error("encode runtime directory", path.to_path_buf(), source))?;
    open_path_owned(
        &encoded,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
    )
    .map_err(|source| directory_open_error(path, source))
}

fn open_directory_at(parent_fd: RawFd, name: &CStr, path: &Path) -> Result<OwnedFd, RvError> {
    openat_owned(
        parent_fd,
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        0,
    )
    .map_err(|source| directory_open_error(path, source))
}

fn directory_open_error(path: &Path, source: io::Error) -> RvError {
    if matches!(
        source.raw_os_error(),
        Some(libc::ELOOP | libc::ENOTDIR | libc::EACCES)
    ) {
        RvError::HostileDir {
            path: path.to_path_buf(),
            reason: format!("refused without following or trusting the entry: {source}"),
        }
    } else {
        io_error("open runtime directory", path.to_path_buf(), source)
    }
}

fn validate_directory_fd(path: &Path, fd: &OwnedFd, uid: u32) -> Result<(), RvError> {
    let stat = fstat_fd(fd.as_raw_fd())
        .map_err(|source| io_error("stat runtime directory", path.to_path_buf(), source))?;
    validate_directory_fields(path, stat.st_uid, stat.st_mode, uid)
}

fn validate_directory_fields(
    path: &Path,
    actual_uid: libc::uid_t,
    actual_mode: libc::mode_t,
    expected_uid: u32,
) -> Result<(), RvError> {
    if actual_mode & libc::S_IFMT != libc::S_IFDIR {
        return Err(RvError::HostileDir {
            path: path.to_path_buf(),
            reason: format!("entry is not a directory (mode {actual_mode:#o})"),
        });
    }
    if actual_uid != expected_uid {
        return Err(RvError::HostileDir {
            path: path.to_path_buf(),
            reason: format!("owned by uid {actual_uid}, expected effective uid {expected_uid}"),
        });
    }

    let permissions = actual_mode & 0o7777;
    if permissions != DIRECTORY_MODE {
        return Err(RvError::HostileDir {
            path: path.to_path_buf(),
            reason: format!("mode is {permissions:#o}, expected exactly {DIRECTORY_MODE:#o}"),
        });
    }
    Ok(())
}

fn runtime_directory_path(dir: &ValidatedRuntimeDir) -> Result<PathBuf, RvError> {
    runtime_directory_path_impl(dir.0.as_raw_fd()).map_err(|source| RvError::Io {
        operation: "resolve validated runtime-directory handle",
        path: PathBuf::from(format!("fd:{}", dir.0.as_raw_fd())),
        source,
    })
}

#[cfg(target_os = "linux")]
fn runtime_directory_path_impl(fd: RawFd) -> io::Result<PathBuf> {
    std::fs::read_link(format!("/proc/self/fd/{fd}"))
}

#[cfg(target_os = "macos")]
fn runtime_directory_path_impl(fd: RawFd) -> io::Result<PathBuf> {
    let mut buffer = [0 as libc::c_char; libc::PATH_MAX as usize];
    // SAFETY: `fd` is a live directory descriptor, `buffer` is writable for
    // `PATH_MAX` bytes, and `F_GETPATH` does not retain either argument.
    let result = unsafe { libc::fcntl(fd, libc::F_GETPATH, buffer.as_mut_ptr()) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    let length = buffer.iter().position(|byte| *byte == 0).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "F_GETPATH result was not NUL-terminated",
        )
    })?;
    let bytes = buffer[..length].iter().map(|byte| *byte as u8).collect();
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

fn io_error(operation: &'static str, path: PathBuf, source: io::Error) -> RvError {
    RvError::Io {
        operation,
        path,
        source,
    }
}

fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem path contains an interior NUL byte",
        )
    })
}

fn component_cstring(component: &OsStr) -> io::Result<CString> {
    CString::new(component.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem component contains an interior NUL byte",
        )
    })
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` takes no arguments, accesses no Rust memory, and has no
    // failure mode.
    unsafe { libc::geteuid() }
}

fn mkdir_path(path: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: `path` is NUL-terminated for the duration of the call, and
    // `mkdir` does not retain its pointer.
    let result = unsafe { libc::mkdir(path.as_ptr(), mode) };
    zero_result(result)
}

fn mkdirat_child(parent_fd: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: `parent_fd` is a live directory descriptor, `name` is
    // NUL-terminated for the call, and `mkdirat` retains neither.
    let result = unsafe { libc::mkdirat(parent_fd, name.as_ptr(), mode) };
    zero_result(result)
}

fn open_path_owned(path: &CStr, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: `path` is NUL-terminated for the call, and `open` does not retain
    // its pointer.
    let result = unsafe { libc::open(path.as_ptr(), flags) };
    owned_fd_result(result)
}

fn openat_owned(
    parent_fd: RawFd,
    name: &CStr,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<OwnedFd> {
    // SAFETY: `parent_fd` is a live directory descriptor, `name` is
    // NUL-terminated for the call, and the variadic mode argument is supplied
    // whenever creation flags can consume it. `openat` retains neither.
    let result = unsafe { libc::openat(parent_fd, name.as_ptr(), flags, mode) };
    owned_fd_result(result)
}

fn owned_fd_result(result: libc::c_int) -> io::Result<OwnedFd> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a successful `open`/`openat` returns a new descriptor whose
        // ownership is transferred exactly once into this `OwnedFd`.
        Ok(unsafe { OwnedFd::from_raw_fd(result) })
    }
}

fn fstat_fd(fd: RawFd) -> io::Result<libc::stat> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `fd` is live and `stat` points to enough writable storage;
    // `fstat` initializes that storage on success and retains no pointer.
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: successful `fstat` initialized every field of `stat`.
        Ok(unsafe { stat.assume_init() })
    }
}

fn fstatat_nofollow(parent_fd: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `parent_fd` is live, `name` is NUL-terminated, `stat` has enough
    // writable storage, and `fstatat` retains no pointer.
    let result = unsafe {
        libc::fstatat(
            parent_fd,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: successful `fstatat` initialized every field of `stat`.
        Ok(unsafe { stat.assume_init() })
    }
}

fn linkat_no_replace(parent_fd: RawFd, old_name: &CStr, new_name: &CStr) -> io::Result<()> {
    // SAFETY: `parent_fd` is live, both names are NUL-terminated for the call,
    // and `linkat` retains no descriptor or pointer.
    let result = unsafe {
        libc::linkat(
            parent_fd,
            old_name.as_ptr(),
            parent_fd,
            new_name.as_ptr(),
            0,
        )
    };
    zero_result(result)
}

fn unlinkat_child(parent_fd: RawFd, name: &CStr) -> io::Result<()> {
    // SAFETY: `parent_fd` is live, `name` is NUL-terminated for the call, and
    // `unlinkat` retains neither descriptor nor pointer.
    let result = unsafe { libc::unlinkat(parent_fd, name.as_ptr(), 0) };
    zero_result(result)
}

fn zero_result(result: libc::c_int) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

enum LinkOutcome {
    Installed,
    AlreadyExists,
}

struct TempEntry {
    parent_fd: RawFd,
    name: CString,
    path: PathBuf,
    active: bool,
}

impl TempEntry {
    fn new(parent_fd: RawFd, name: CString, path: PathBuf) -> Self {
        Self {
            parent_fd,
            name,
            path,
            active: true,
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }

    fn discard(&mut self) -> Result<(), RvError> {
        if !self.active {
            return Ok(());
        }
        unlinkat_child(self.parent_fd, &self.name)
            .map_err(|source| io_error("discard sentinel temp", self.path.clone(), source))?;
        self.active = false;
        Ok(())
    }
}

impl Drop for TempEntry {
    fn drop(&mut self) {
        if self.active && unlinkat_child(self.parent_fd, &self.name).is_ok() {
            self.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs::{self, Permissions};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::{
        ControllerInputUnavailable, ControllerSocketStatus, DIRECTORY_MODE, RvError,
        SOCKET_PATH_LIMIT, effective_uid, open_runtime_dir_at, preflight_len,
        prepare_controller_socket, runtime_base, validate_directory_fields,
    };
    use crate::lockfile::{OwnerLock, state_root_in, try_acquire};
    use crate::session_key::{SessionKey, encode};

    fn session_key(name: &str) -> SessionKey {
        encode(name).unwrap()
    }

    fn runtime_in(temp: &TempDir) -> (PathBuf, super::ValidatedRuntimeDir) {
        let injected = temp.path().join("runtime");
        let base = runtime_base(Some(injected.as_os_str()), None, effective_uid());
        let dir = open_runtime_dir_at(&base).unwrap();
        (base, dir)
    }

    fn owner_lock(temp: &TempDir, key: &SessionKey) -> OwnerLock {
        let root = state_root_in(temp.path(), key).unwrap();
        try_acquire(&root).unwrap().unwrap()
    }

    fn socket_path(base: &Path, key: &SessionKey) -> PathBuf {
        base.join("herdr-top")
            .join(format!("{}.sock", key.hash16()))
    }

    fn sentinel_path(base: &Path, key: &SessionKey) -> PathBuf {
        base.join("herdr-top")
            .join(format!("{}.name", key.hash16()))
    }

    #[test]
    fn hostile_symlink_refused() {
        let temp = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let injected = temp.path().join("runtime");
        let base = runtime_base(Some(injected.as_os_str()), None, effective_uid());
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, Permissions::from_mode(0o700)).unwrap();
        symlink(elsewhere.path(), base.join("herdr-top")).unwrap();

        assert!(matches!(
            open_runtime_dir_at(&base),
            Err(RvError::HostileDir { .. })
        ));
        assert_eq!(fs::read_dir(elsewhere.path()).unwrap().count(), 0);
        assert!(base.join("herdr-top").is_symlink());
    }

    #[test]
    fn wrong_mode_refused() {
        let temp = tempfile::tempdir().unwrap();
        let injected = temp.path().join("runtime");
        let base = runtime_base(Some(injected.as_os_str()), None, effective_uid());
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, Permissions::from_mode(0o700)).unwrap();
        let child = base.join("herdr-top");
        fs::create_dir(&child).unwrap();
        fs::set_permissions(&child, Permissions::from_mode(0o755)).unwrap();

        assert!(matches!(
            open_runtime_dir_at(&base),
            Err(RvError::HostileDir { .. })
        ));
        assert_eq!(
            fs::symlink_metadata(&child).unwrap().permissions().mode() & 0o7777,
            0o755
        );
    }

    #[test]
    fn wrong_owner_refused() {
        let expected_uid = effective_uid();
        let foreign_uid = expected_uid.wrapping_add(1);

        assert!(matches!(
            validate_directory_fields(
                Path::new("synthetic-runtime-dir"),
                foreign_uid,
                libc::S_IFDIR | DIRECTORY_MODE,
                expected_uid,
            ),
            Err(RvError::HostileDir { .. })
        ));
        eprintln!(
            "end-to-end wrong-owner setup skipped: changing directory ownership requires root"
        );
    }

    #[test]
    fn abandoned_temp_is_inert() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("abandoned temp session");
        let (base, dir) = runtime_in(&runtime_temp);
        let lock = owner_lock(&lock_temp, &key);
        let abandoned = base.join("herdr-top").join(format!(
            "{}.name.tmp.1234.01ARZ3NDEKTSV4RRFFQ69G5FAV",
            key.hash16()
        ));
        fs::write(&abandoned, b"abandoned").unwrap();

        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::NotImplementedInVerticalSlice
        );
        assert_eq!(fs::read(&abandoned).unwrap(), b"abandoned");
        assert_eq!(
            fs::read(sentinel_path(&base, &key)).unwrap(),
            key.name().as_bytes()
        );
    }

    #[test]
    fn collision_is_error_not_stale() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("collision target");
        let (base, dir) = runtime_in(&runtime_temp);
        let lock = owner_lock(&lock_temp, &key);
        let socket = socket_path(&base, &key);
        let sentinel = sentinel_path(&base, &key);
        let listener = UnixListener::bind(&socket).unwrap();
        drop(listener);
        fs::write(&sentinel, b"different session").unwrap();

        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::Unavailable(ControllerInputUnavailable::Collision {
                found: "different session".to_owned(),
            })
        );
        assert!(socket.exists());
        assert_eq!(fs::read(&sentinel).unwrap(), b"different session");
    }

    #[test]
    fn orphan_never_unlinked() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("orphan target");
        let (base, dir) = runtime_in(&runtime_temp);
        let lock = owner_lock(&lock_temp, &key);
        let socket = socket_path(&base, &key);
        let sentinel = sentinel_path(&base, &key);
        let listener = UnixListener::bind(&socket).unwrap();
        drop(listener);

        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::Unavailable(ControllerInputUnavailable::UnsafeOrphan)
        );
        assert!(socket.exists());
        assert!(!sentinel.exists());
    }

    #[test]
    fn stale_socket_removed_under_lock() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("stale and live target");
        let (base, dir) = runtime_in(&runtime_temp);
        let lock = owner_lock(&lock_temp, &key);
        let socket = socket_path(&base, &key);
        fs::write(sentinel_path(&base, &key), key.name().as_bytes()).unwrap();

        let stale_listener = UnixListener::bind(&socket).unwrap();
        drop(stale_listener);
        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::NotImplementedInVerticalSlice
        );
        assert!(!socket.exists());

        let live_listener = UnixListener::bind(&socket).unwrap();
        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::Unavailable(ControllerInputUnavailable::LiveEndpointUnderLock)
        );
        assert!(socket.exists());
        drop(live_listener);
    }

    #[test]
    fn path_length_preflight() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("long runtime path");
        let component = "x".repeat(SOCKET_PATH_LIMIT);
        let injected = runtime_temp.path().join(component);
        let base = runtime_base(Some(injected.as_os_str()), None, effective_uid());
        let dir = open_runtime_dir_at(&base).unwrap();
        let lock = owner_lock(&lock_temp, &key);
        let long_socket = socket_path(&base, &key);

        assert!(matches!(
            preflight_len(&long_socket),
            Err(RvError::PathTooLong { .. })
        ));
        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::Unavailable(ControllerInputUnavailable::PathTooLong)
        );
        assert!(preflight_len(Path::new("/tmp/herdr-top/short.sock")).is_ok());
        assert_eq!(
            runtime_base(
                Some(OsStr::new("")),
                Some(runtime_temp.path().as_os_str()),
                42
            ),
            runtime_temp.path().join("herdr-top-42")
        );
        assert_eq!(
            runtime_base(
                Some(runtime_temp.path().as_os_str()),
                Some(OsStr::new("/ignored")),
                42
            ),
            runtime_temp.path()
        );
    }

    #[test]
    fn ready_records_not_implemented_status() {
        let runtime_temp = tempfile::tempdir().unwrap();
        let lock_temp = tempfile::tempdir().unwrap();
        let key = session_key("Exact Café session");
        let (base, dir) = runtime_in(&runtime_temp);
        let lock = owner_lock(&lock_temp, &key);
        let socket = socket_path(&base, &key);
        let sentinel = sentinel_path(&base, &key);

        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::NotImplementedInVerticalSlice
        );
        assert_eq!(fs::read(&sentinel).unwrap(), key.name().as_bytes());
        assert!(!socket.exists());
        assert_eq!(
            prepare_controller_socket(&dir, &key, &lock).unwrap(),
            ControllerSocketStatus::NotImplementedInVerticalSlice
        );
        assert_eq!(fs::read(&sentinel).unwrap(), key.name().as_bytes());
        assert!(!socket.exists());
    }
}
