//! Exercises scripts/fetch-release.sh in local-source and no-pins modes.

use std::process::Command;

struct LocalFixture {
    dir: std::path::PathBuf,
    workdir: std::path::PathBuf,
}

fn make_fixture(corrupt: bool) -> LocalFixture {
    let base =
        std::env::temp_dir().join(format!("herdr-top-fetch-{}-{corrupt}", std::process::id()));
    let dir = base.join("artifacts");
    let workdir = base.join("plugin");
    std::fs::create_dir_all(&dir).expect("artifact dir");
    std::fs::create_dir_all(&workdir).expect("plugin dir");
    let staging = base.join("stage");
    std::fs::create_dir_all(&staging).expect("stage dir");
    std::fs::write(staging.join("herdr-top"), b"#!/bin/sh\necho stub-0.1.0\n")
        .expect("stub binary");
    let target = current_target();
    let archive = dir.join(format!("herdr-top-0.1.0-{target}.tar.gz"));
    let tar = Command::new("tar")
        .args(["-czf"])
        .arg(&archive)
        .args(["-C"])
        .arg(&staging)
        .arg("herdr-top")
        .status()
        .expect("tar runs");
    assert!(tar.success());
    let bytes = std::fs::read(&archive).expect("archive bytes");
    let digest = sha256_hex(&bytes);
    let sums = if corrupt {
        format!("{:0>64}  herdr-top-0.1.0-{target}.tar.gz\n", "0")
    } else {
        format!("{digest}  herdr-top-0.1.0-{target}.tar.gz\n")
    };
    std::fs::write(dir.join("SHA256SUMS"), sums).expect("sums file");
    LocalFixture { dir, workdir }
}

fn current_target() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        other => panic!("unsupported test platform: {other:?}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let out = Command::new("sh")
        .arg("-c")
        .arg("sha256sum 2>/dev/null || shasum -a 256")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("stdin")
                .write_all(bytes)
                .expect("write");
            child.wait_with_output()
        })
        .expect("digest tool runs");
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .expect("digest")
        .to_owned()
}

fn run_fetch(fixture: &LocalFixture) -> std::process::Output {
    Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/fetch-release.sh"
        ))
        .current_dir(&fixture.workdir)
        .env("HERDR_TOP_FETCH_LOCAL_DIR", &fixture.dir)
        .env("HERDR_TOP_FETCH_LOCAL_VERSION", "0.1.0")
        .output()
        .expect("script runs")
}

#[test]
fn i7_local_source_install_places_executable_binary() {
    let fixture = make_fixture(false);
    let out = run_fetch(&fixture);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let installed = fixture.workdir.join("bin/herdr-top");
    assert!(installed.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = installed.metadata().expect("metadata").permissions().mode();
        assert_ne!(mode & 0o111, 0, "binary must be executable");
    }
}

#[test]
fn i7_checksum_mismatch_fails_and_installs_nothing_and_keeps_the_source() {
    let fixture = make_fixture(true);
    let out = run_fetch(&fixture);
    assert_ne!(out.status.code(), Some(0));
    assert!(!fixture.workdir.join("bin/herdr-top").exists());
    let target = current_target();
    assert!(
        fixture
            .dir
            .join(format!("herdr-top-0.1.0-{target}.tar.gz"))
            .exists(),
        "local-mode source archives must never be deleted"
    );
}

#[test]
fn i7_without_pins_and_without_local_source_the_script_fails_closed() {
    let base = std::env::temp_dir().join(format!("herdr-top-nopin-{}", std::process::id()));
    std::fs::create_dir_all(&base).expect("dir");
    let empty_pins = base.join("release-pins.env");
    std::fs::write(&empty_pins, "HERDR_TOP_RELEASE_VERSION=\"\"\n").expect("pins file");
    let out = Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/scripts/fetch-release.sh"
        ))
        .current_dir(&base)
        .env("HERDR_TOP_FETCH_PINS_FILE", &empty_pins)
        .output()
        .expect("script runs");
    assert_ne!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stderr).contains("no release pinned"));
}
