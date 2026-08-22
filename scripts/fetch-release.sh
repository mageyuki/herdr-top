#!/usr/bin/env bash
# herdr plugin [[build]] command: fetches the pinned release artifact for the
# current platform, verifies its sha256 against repo-committed pins, and
# installs bin/herdr-top. Requires no Rust toolchain. Never touches PATH or
# /dev/tty. See docs/guides/release-process.md for the pin lifecycle.
#
# CI-only local-source mode: HERDR_TOP_FETCH_LOCAL_DIR (holding the archive
# and SHA256SUMS) + HERDR_TOP_FETCH_LOCAL_VERSION verify a just-built archive
# without any network access. HERDR_TOP_FETCH_PINS_FILE overrides the pins
# path (tests use it to stay hermetic).
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

fail() { printf 'fetch-release: error: %s\n' "$1" >&2; exit 1; }

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64) target=x86_64-unknown-linux-gnu; pin_var=HERDR_TOP_SHA256_X86_64_UNKNOWN_LINUX_GNU ;;
  Linux/aarch64) target=aarch64-unknown-linux-gnu; pin_var=HERDR_TOP_SHA256_AARCH64_UNKNOWN_LINUX_GNU ;;
  Darwin/x86_64) target=x86_64-apple-darwin; pin_var=HERDR_TOP_SHA256_X86_64_APPLE_DARWIN ;;
  Darwin/arm64) target=aarch64-apple-darwin; pin_var=HERDR_TOP_SHA256_AARCH64_APPLE_DARWIN ;;
  *) fail "unsupported platform: $(uname -s)/$(uname -m)" ;;
esac

digest_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    fail "no sha256 tool available"
  fi
}

workdir=$PWD
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

downloaded=""
if [[ -n ${HERDR_TOP_FETCH_LOCAL_DIR-} ]]; then
  version=${HERDR_TOP_FETCH_LOCAL_VERSION:?local mode needs HERDR_TOP_FETCH_LOCAL_VERSION}
  archive_name="herdr-top-$version-$target.tar.gz"
  archive="$HERDR_TOP_FETCH_LOCAL_DIR/$archive_name"
  [[ -f $archive ]] || fail "local archive missing: $archive_name"
  expected=$(awk -v name="$archive_name" '$2 == name {print $1}' \
    "$HERDR_TOP_FETCH_LOCAL_DIR/SHA256SUMS")
  [[ -n $expected ]] || fail "no SHA256SUMS entry for $archive_name"
else
  pins_file=${HERDR_TOP_FETCH_PINS_FILE:-"$script_dir/release-pins.env"}
  # shellcheck source=release-pins.env
  source "$pins_file"
  version=${HERDR_TOP_RELEASE_VERSION-}
  [[ -n $version ]] || fail "no release pinned yet (release pins are empty)"
  expected=${!pin_var-}
  [[ -n $expected ]] || fail "no checksum pinned for $target"
  archive_name="herdr-top-$version-$target.tar.gz"
  archive="$tmpdir/$archive_name"
  url="https://github.com/mageyuki/herdr-top/releases/download/v$version/$archive_name"
  curl --fail --location --silent --show-error --retry 3 --retry-delay 2 \
    --output "$archive" "$url" || fail "download failed: $url"
  downloaded="$archive"
fi

actual=$(digest_of "$archive")
if [[ $actual != "$expected" ]]; then
  # Remove only a bad DOWNLOAD; a local-mode source archive belongs to the
  # caller (in CI it is the build output staged for upload).
  [[ -z $downloaded ]] || rm -f "$downloaded"
  fail "checksum mismatch for $archive_name (expected $expected, got $actual)"
fi

mkdir -p "$workdir/bin"
tar -xzf "$archive" -C "$tmpdir" herdr-top
install -m 0755 "$tmpdir/herdr-top" "$workdir/bin/herdr-top"
printf 'fetch-release: installed bin/herdr-top (%s, %s)\n' "$version" "$target"
