#![allow(unsafe_code)]
//! Per-session state-root discovery, name sentinel, and advisory owner lock.

use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::session_key::SessionKey;

const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const SENTINEL_FILE: &str = "session-name.txt";
const LOCK_FILE: &str = "collector.lock";

/// The per-session persistent state directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot(pub PathBuf);

/// An exclusive advisory lock held by its open file descriptor.
#[derive(Debug)]
pub struct OwnerLock {
    _file: File,
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        // Explicitly unlock the shared open file description so an inherited
        // descriptor cannot extend the lock lifetime across fork-before-exec.
        // Closing the owned descriptor remains the backstop if unlocking fails.
        let _ = flock_unlock(&self._file);
    }
}

/// Diagnostic information about the current session owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRecord {
    /// The owner's process identifier.
    pub pid: u32,
    /// The owner's Unix-epoch start time in milliseconds.
    pub started_at_ms: i64,
    /// The Herdr terminal identifier associated with the owner.
    pub terminal_id: Option<String>,
    /// The owner's last known public pane identifier.
    pub pane_id: Option<String>,
}

/// Read-only availability of an already-existing owner-lock file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingLockVerdict {
    /// No lock file exists yet.
    Missing,
    /// Another open file description currently holds the lock.
    Held,
    /// The existing private lock file can be locked and released.
    Available,
    /// The entry is not a private, own-uid, single-link regular file.
    MalformedOrUnsafe,
    /// The existing entry cannot be safely opened, inspected, locked, or unlocked.
    Unreadable,
}

/// Errors from state-root initialization and owner-lock acquisition.
#[derive(Debug, Error)]
pub enum LockError {
    /// Neither XDG_STATE_HOME nor HOME supplies a non-empty state base.
    #[error("state root cannot be resolved because XDG_STATE_HOME and HOME are unset or empty")]
    NoResolvableBase,
    /// The state directory belongs to a different exact session name.
    #[error("session-name sentinel at {path:?} does not match {expected:?}; found bytes {found:?}")]
    NameMismatch {
        /// The sentinel path that did not match.
        path: PathBuf,
        /// The exact session name expected by the resolved key.
        expected: String,
        /// The bytes read from the existing sentinel.
        found: Vec<u8>,
    },
    /// A filesystem or advisory-lock operation failed.
    #[error("I/O error at {path:?}: {source}")]
    Io {
        /// The path involved in the failed operation.
        path: PathBuf,
        /// The underlying operating-system error.
        #[source]
        source: io::Error,
    },
}

/// Resolves and initializes the current session's persistent state directory.
///
/// `XDG_STATE_HOME` wins when non-empty; otherwise this uses
/// `$HOME/.local/state`.
pub fn state_root(key: &SessionKey) -> Result<StateRoot, LockError> {
    let xdg_state_home = env::var_os("XDG_STATE_HOME");
    let home = env::var_os("HOME");
    let base = resolve_state_base(xdg_state_home.as_deref(), home.as_deref())?;

    state_root_in(&base, key)
}

/// Resolves the state base without creating, changing, or resolving any path.
///
/// A non-empty `xdg_state_home` wins; otherwise this derives `.local/state`
/// beneath a non-empty `home`.
pub fn resolve_state_base(
    xdg_state_home: Option<&OsStr>,
    home: Option<&OsStr>,
) -> Result<PathBuf, LockError> {
    if let Some(xdg_state_home) = xdg_state_home.filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(xdg_state_home));
    }
    home.filter(|value| !value.is_empty())
        .map(|home| PathBuf::from(home).join(".local/state"))
        .ok_or(LockError::NoResolvableBase)
}

/// Derives a session state root without inspecting or changing the filesystem.
#[must_use]
pub fn derive_state_root(base: &Path, key: &SessionKey) -> StateRoot {
    StateRoot(base.join("herdr-top").join("sessions").join(key.encoded()))
}

/// Initializes a session state directory beneath an explicit, environment-free base.
pub fn state_root_in(base: &Path, key: &SessionKey) -> Result<StateRoot, LockError> {
    let root = derive_state_root(base, key);
    create_private_directory(&root.0)?;

    let sentinel = root.0.join(SENTINEL_FILE);
    validate_or_create_sentinel(&sentinel, key)?;

    Ok(root)
}

/// Probes only an already-existing owner-lock file and never writes or creates.
///
/// An available lock is explicitly released before this function returns.
#[must_use]
pub fn probe_existing_lock(root: &StateRoot) -> ExistingLockVerdict {
    let path = root.0.join(LOCK_FILE);
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return ExistingLockVerdict::Missing;
        }
        Err(source) if source.raw_os_error() == Some(libc::ELOOP) => {
            return ExistingLockVerdict::MalformedOrUnsafe;
        }
        Err(_) => return ExistingLockVerdict::Unreadable,
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return ExistingLockVerdict::Unreadable,
    };
    if !metadata.file_type().is_file()
        || metadata.mode() & 0o7777 != FILE_MODE
        || metadata.uid() != effective_uid()
        || metadata.nlink() != 1
    {
        return ExistingLockVerdict::MalformedOrUnsafe;
    }

    match flock_exclusive_nonblocking(&file) {
        Ok(false) => ExistingLockVerdict::Held,
        Ok(true) => {
            if flock_unlock(&file).is_ok() {
                ExistingLockVerdict::Available
            } else {
                ExistingLockVerdict::Unreadable
            }
        }
        Err(_) => ExistingLockVerdict::Unreadable,
    }
}

/// Attempts to acquire the session owner lock without blocking.
///
/// Returns `Ok(None)` when another open file description holds the lock.
pub fn try_acquire(root: &StateRoot) -> Result<Option<OwnerLock>, LockError> {
    let path = root.0.join(LOCK_FILE);
    let file = open_private_lock_file(&path)?;
    let acquired = flock_exclusive_nonblocking(&file).map_err(|source| LockError::Io {
        path: path.clone(),
        source,
    })?;

    if acquired {
        Ok(Some(OwnerLock { _file: file }))
    } else {
        Ok(None)
    }
}

fn create_private_directory(path: &Path) -> Result<(), LockError> {
    match fs::create_dir(path) {
        Ok(()) => set_mode(path, DIRECTORY_MODE),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            let Some(parent) = path.parent().filter(|parent| *parent != path) else {
                return Err(LockError::Io {
                    path: path.to_path_buf(),
                    source,
                });
            };
            create_private_directory(parent)?;
            create_private_directory_once(path)
        }
        Err(source) => Err(LockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn create_private_directory_once(path: &Path) -> Result<(), LockError> {
    match fs::create_dir(path) {
        Ok(()) => set_mode(path, DIRECTORY_MODE),
        // A racing initializer created this directory, so it is pre-existing
        // from this call's perspective and must not be chmodded.
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(source) => Err(LockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn set_mode(path: &Path, mode: u32) -> Result<(), LockError> {
    fs::set_permissions(path, Permissions::from_mode(mode)).map_err(|source| LockError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_or_create_sentinel(path: &Path, key: &SessionKey) -> Result<(), LockError> {
    match fs::read(path) {
        Ok(found) => validate_sentinel(path, key, found),
        // A missing sentinel is treated as an interrupted first creation. The
        // initializer completes it before anything else in the directory is used.
        Err(source) if source.kind() == io::ErrorKind::NotFound => create_sentinel(path, key),
        Err(source) => Err(LockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn create_sentinel(path: &Path, key: &SessionKey) -> Result<(), LockError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(FILE_MODE);

    match options.open(path) {
        Ok(mut file) => {
            file.set_permissions(Permissions::from_mode(FILE_MODE))
                .map_err(|source| LockError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
            file.write_all(key.name().as_bytes())
                .map_err(|source| LockError::Io {
                    path: path.to_path_buf(),
                    source,
                })
        }
        // Another initializer won the create-new race; validate its exact name.
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            let found = fs::read(path).map_err(|source| LockError::Io {
                path: path.to_path_buf(),
                source,
            })?;
            validate_sentinel(path, key, found)
        }
        Err(source) => Err(LockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn validate_sentinel(path: &Path, key: &SessionKey, found: Vec<u8>) -> Result<(), LockError> {
    if found == key.name().as_bytes() {
        Ok(())
    } else {
        Err(LockError::NameMismatch {
            path: path.to_path_buf(),
            expected: key.name().to_owned(),
            found,
        })
    }
}

fn open_private_lock_file(path: &Path) -> Result<File, LockError> {
    let mut create_options = OpenOptions::new();
    create_options
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE);

    match create_options.open(path) {
        Ok(file) => {
            file.set_permissions(Permissions::from_mode(FILE_MODE))
                .map_err(|source| LockError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
            Ok(file)
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| LockError::Io {
                path: path.to_path_buf(),
                source,
            }),
        Err(source) => Err(LockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn flock_exclusive_nonblocking(file: &File) -> io::Result<bool> {
    // SAFETY: `file` owns a valid descriptor for this call, and `flock` neither
    // retains the descriptor nor dereferences any Rust memory.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }

    let source = io::Error::last_os_error();
    if source
        .raw_os_error()
        .is_some_and(|errno| errno == libc::EWOULDBLOCK || errno == libc::EAGAIN)
    {
        Ok(false)
    } else {
        Err(source)
    }
}

fn flock_unlock(file: &File) -> io::Result<()> {
    // SAFETY: `file` owns a valid descriptor for this call, and `flock` neither
    // retains the descriptor nor dereferences any Rust memory.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` takes no arguments, accesses no Rust memory, and has no
    // failure mode.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::process::{Child, Command};

    use tempfile::tempdir;

    use super::{state_root_in, try_acquire};
    use crate::session_key::encode;

    #[test]
    fn owner_lock_drop_releases_for_same_process_reacquire() {
        let temp = tempdir().unwrap();
        let key = encode("same-process drop test").unwrap();
        let root = state_root_in(temp.path(), &key).unwrap();
        let owner = try_acquire(&root)
            .unwrap()
            .expect("first owner should acquire the lock");

        drop(owner);

        assert!(
            try_acquire(&root).unwrap().is_some(),
            "dropping the owner should release the lock"
        );
    }

    #[test]
    fn owner_lock_drop_releases_with_inherited_descriptor() {
        let temp = tempdir().unwrap();
        let key = encode("inherited-descriptor drop test").unwrap();
        let root = state_root_in(temp.path(), &key).unwrap();
        let owner = try_acquire(&root)
            .unwrap()
            .expect("first owner should acquire the lock");
        let inherited = duplicate_without_close_on_exec(&owner._file).unwrap();
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 5"])
            .spawn()
            .expect("child should spawn");

        drop(inherited);
        drop(owner);
        let child_state = child.try_wait();
        let reacquired = try_acquire(&root);
        kill_and_reap(&mut child);

        assert!(
            child_state.unwrap().is_none(),
            "child should still be running during the re-acquire attempt"
        );
        assert!(
            reacquired.unwrap().is_some(),
            "dropping the owner should unlock an inherited open file description"
        );
    }

    #[test]
    fn owner_lock_file_is_close_on_exec_by_default() {
        let temp = tempdir().unwrap();
        let key = encode("close-on-exec drop test").unwrap();
        let root = state_root_in(temp.path(), &key).unwrap();
        let owner = try_acquire(&root)
            .unwrap()
            .expect("first owner should acquire the lock");
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 5"])
            .spawn()
            .expect("child should spawn");

        drop(owner);
        let child_state = child.try_wait();
        let reacquired = try_acquire(&root);
        kill_and_reap(&mut child);

        assert!(
            child_state.unwrap().is_none(),
            "child should still be running during the re-acquire attempt"
        );
        assert!(
            reacquired.unwrap().is_some(),
            "the lock descriptor should be closed across an ordinary exec"
        );
    }

    fn duplicate_without_close_on_exec(file: &File) -> io::Result<File> {
        // SAFETY: `file` owns a valid descriptor for this call. On success,
        // `dup` returns a distinct descriptor that this function takes ownership of.
        let descriptor = unsafe { libc::dup(file.as_raw_fd()) };
        if descriptor == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `descriptor` was just returned by `dup`, is not owned by any
        // other `File`, and ownership is transferred exactly once here.
        let duplicate = unsafe { File::from_raw_fd(descriptor) };

        // SAFETY: `duplicate` owns a valid descriptor, and `F_GETFD` neither
        // retains the descriptor nor dereferences any Rust memory.
        let flags = unsafe { libc::fcntl(duplicate.as_raw_fd(), libc::F_GETFD) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `duplicate` owns a valid descriptor, and `F_SETFD` only
        // updates that descriptor's flags for the duration of this call.
        let result = unsafe {
            libc::fcntl(
                duplicate.as_raw_fd(),
                libc::F_SETFD,
                flags & !libc::FD_CLOEXEC,
            )
        };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(duplicate)
    }

    fn kill_and_reap(child: &mut Child) {
        let kill_result = child.kill();
        let wait_result = child.wait();

        kill_result.expect("child should be killed");
        wait_result.expect("child should be reaped");
    }
}
