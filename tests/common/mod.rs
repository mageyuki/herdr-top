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
