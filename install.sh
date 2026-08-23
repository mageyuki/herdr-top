#!/usr/bin/env bash
# Install the latest herdr-top release, or set HERDR_TOP_VERSION (for example,
# HERDR_TOP_VERSION=0.1.0) to install an explicit version without a leading v.
#
# Linux assets use *-unknown-linux-gnu and are built on Ubuntu 24.04 runners.
# They require glibc; the workflow does not establish compatibility with older
# glibc systems, so no more precise compatibility floor is promised here.
set -euo pipefail


main() {
  repo=https://github.com/mageyuki/herdr-top

  fail() {
    printf 'install: error: %s\n' "$1" >&2
    exit 1
  }

  command -v curl >/dev/null 2>&1 || fail "install requires curl"

  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) target=x86_64-unknown-linux-gnu ;;
    Linux/aarch64) target=aarch64-unknown-linux-gnu ;;
    Darwin/x86_64) target=x86_64-apple-darwin ;;
    Darwin/arm64) target=aarch64-apple-darwin ;;
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

  download() {
    local url=$1
    local destination=$2
    curl --fail --location --silent --show-error --retry 3 --retry-delay 2 \
      --output "$destination" "$url" || fail "download failed: $url"
  }

  tmpdir=$(mktemp -d)
  install_tmp=
  cleanup() {
    if [[ -n $install_tmp ]]; then
      rm -f "$install_tmp"
    fi
    rm -rf "$tmpdir"
  }
  trap cleanup EXIT

  checksums=$tmpdir/SHA256SUMS
  version=${HERDR_TOP_VERSION-}
  if [[ -n $version ]]; then
    [[ $version =~ ^[0-9A-Za-z][0-9A-Za-z.-]*$ ]] || \
      fail "invalid HERDR_TOP_VERSION: use the release version without a leading v"
    release_url=$repo/releases/download/v$version
    archive_name=herdr-top-$version-$target.tar.gz
  else
    release_url=$repo/releases/latest/download
  fi

  download "$release_url/SHA256SUMS" "$checksums"

  if [[ -z $version ]]; then
    match_count=$(awk -v suffix="-$target.tar.gz" \
      '$2 ~ /^herdr-top-/ && substr($2, length($2) - length(suffix) + 1) == suffix { count++ } END { print count + 0 }' \
      "$checksums")
    [[ $match_count == 1 ]] || \
      fail "expected one SHA256SUMS entry for $target, found $match_count"
    archive_name=$(awk -v suffix="-$target.tar.gz" \
      '$2 ~ /^herdr-top-/ && substr($2, length($2) - length(suffix) + 1) == suffix { print $2 }' \
      "$checksums")
    [[ $archive_name != */* ]] || fail "invalid archive name in SHA256SUMS"
    version=${archive_name#herdr-top-}
    version=${version%-$target.tar.gz}
  fi

  expected=$(awk -v name="$archive_name" '$2 == name { print $1 }' "$checksums")
  [[ ${#expected} == 64 && $expected != *[!0-9A-Fa-f]* ]] || \
    fail "no valid SHA256SUMS entry for $archive_name"

  archive=$tmpdir/$archive_name
  download "$release_url/$archive_name" "$archive"
  actual=$(digest_of "$archive")
  if [[ $actual != "$expected" ]]; then
    rm -f "$archive"
    fail "checksum mismatch for $archive_name (expected $expected, got $actual)"
  fi

  tar -xzf "$archive" -C "$tmpdir" herdr-top
  if [[ -n ${INSTALL_DIR-} ]]; then
    install_dir=$INSTALL_DIR
  else
    [[ -n ${HOME-} ]] || fail "HOME is unset; set INSTALL_DIR explicitly"
    install_dir=$HOME/.local/bin
  fi
  mkdir -p "$install_dir"
  install_tmp=$(mktemp "$install_dir/.herdr-top.XXXXXX")
  install -m 0755 "$tmpdir/herdr-top" "$install_tmp"
  mv -f "$install_tmp" "$install_dir/herdr-top"
  install_tmp=

  printf 'install: installed %s (%s, %s)\n' "$install_dir/herdr-top" "$version" "$target"
  printf 'install: check with: %s doctor\n' "$install_dir/herdr-top"
  case ":${PATH-}:" in
    *":$install_dir:"*) ;;
    *) printf 'install: warning: %s is not on PATH\n' "$install_dir" >&2 ;;
  esac
}

main "$@"
