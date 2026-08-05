mod common;

use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt;

use herdr_top::lockfile::{LockError, state_root_in, try_acquire};
use herdr_top::session_key::encode;
use tempfile::tempdir;

const HELPER_BASE_ENV: &str = "HERDR_TOP_T3_HELPER_BASE";
const HELPER_NAME_ENV: &str = "HERDR_TOP_T3_HELPER_NAME";
const TEST_SESSION_NAME: &str = "T3 lock test";

#[test]
fn helper_try_acquire() {
    let Some(base) = std::env::var_os(HELPER_BASE_ENV) else {
        return;
    };
    let name = std::env::var(HELPER_NAME_ENV).expect("helper session name should be set");
    let key = encode(&name).unwrap();
    let root = state_root_in(base.as_ref(), &key).unwrap();

    match try_acquire(&root).unwrap() {
        Some(_owner) => println!("T3_HELPER: ACQUIRED"),
        None => println!("T3_HELPER: HELD"),
    }
}

#[test]
fn second_process_cannot_acquire() {
    let temp = tempdir().unwrap();
    let key = encode(TEST_SESSION_NAME).unwrap();
    let root = state_root_in(temp.path(), &key).unwrap();
    let owner = try_acquire(&root)
        .unwrap()
        .expect("parent should acquire the owner lock");
    let envs = [
        (HELPER_BASE_ENV, temp.path().as_os_str()),
        (HELPER_NAME_ENV, OsStr::new(TEST_SESSION_NAME)),
    ];

    let held = common::spawn_self_test_helper("helper_try_acquire", &envs);
    assert!(
        held.status.success(),
        "helper failed: {}",
        String::from_utf8_lossy(&held.stderr)
    );
    assert!(
        String::from_utf8_lossy(&held.stdout).contains("T3_HELPER: HELD"),
        "unexpected helper stdout: {}",
        String::from_utf8_lossy(&held.stdout)
    );

    drop(owner);
    let acquired = common::spawn_self_test_helper("helper_try_acquire", &envs);
    assert!(
        acquired.status.success(),
        "helper failed: {}",
        String::from_utf8_lossy(&acquired.stderr)
    );
    assert!(
        String::from_utf8_lossy(&acquired.stdout).contains("T3_HELPER: ACQUIRED"),
        "unexpected helper stdout: {}",
        String::from_utf8_lossy(&acquired.stdout)
    );
}

#[test]
fn lock_released_on_drop() {
    let temp = tempdir().unwrap();
    let key = encode("drop test").unwrap();
    let root = state_root_in(temp.path(), &key).unwrap();
    let owner = try_acquire(&root)
        .unwrap()
        .expect("first owner should acquire the lock");

    assert!(try_acquire(&root).unwrap().is_none());
    drop(owner);

    assert!(try_acquire(&root).unwrap().is_some());
}

#[test]
fn name_sentinel_mismatch_refuses() {
    let temp = tempdir().unwrap();
    let key = encode("foo").unwrap();
    let root = state_root_in(temp.path(), &key).unwrap();
    fs::write(root.0.join("session-name.txt"), b"different name").unwrap();

    assert!(matches!(
        state_root_in(temp.path(), &key),
        Err(LockError::NameMismatch { .. })
    ));
}

#[test]
fn modes_are_0700_and_0600() {
    let temp = tempdir().unwrap();
    let key = encode("mode test").unwrap();
    let root = state_root_in(temp.path(), &key).unwrap();
    let herdr_top = temp.path().join("herdr-top");
    let sessions = herdr_top.join("sessions");

    for directory in [&herdr_top, &sessions, &root.0] {
        let mode = fs::metadata(directory).unwrap().permissions().mode();
        assert_eq!(mode & 0o7777, 0o700, "mode for {}", directory.display());
    }

    let sentinel_mode = fs::metadata(root.0.join("session-name.txt"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(sentinel_mode & 0o7777, 0o600);
}
