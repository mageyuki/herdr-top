# Release process

A Herdr Top release is attached to a `v<version>` Git tag. It contains one
archive for each supported target:

- `herdr-top-<version>-aarch64-apple-darwin.tar.gz`
- `herdr-top-<version>-x86_64-apple-darwin.tar.gz`
- `herdr-top-<version>-x86_64-unknown-linux-gnu.tar.gz`
- `herdr-top-<version>-aarch64-unknown-linux-gnu.tar.gz`

The release also contains `SHA256SUMS`, which records the digest of every
archive.

## Understand who performs each step

The user pushes the release tag, publishes the resulting draft as a
pre-release, and later promotes the pre-release. The release workflow builds,
packages, smokes, and checksums the artifacts and creates the draft release.
Pin updates and fixes use the normal pull request flow. No other release step
requires a direct user mutation of release state.

## Dry-run the workflow before tagging

Before the first tag of every release, run the release workflow manually on
the default branch. For the current default branch, run:

```sh
gh workflow run release --ref main
gh run watch
```

Do not create or push the tag until the dry run succeeds. The run must pass all
four native runner legs and the aggregate job. Every runner leg builds its
target archive, computes a local checksum, installs through
`scripts/fetch-release.sh` in local-source mode, and executes the installed
binary. The aggregate job produces `SHA256SUMS` as a workflow artifact.

A `workflow_dispatch` run does not create a GitHub Release, so this check
validates all runner, packaging, extraction, and smoke paths without creating
anything irreversible. Fix any failure through the normal pull request flow,
then repeat the dry run before tagging.

## Publish and pin the release

After the dry run succeeds, complete the release in this order:

1. Push the `v<version>` tag.
2. Wait for the tag-triggered workflow to build all four archives, generate
   `SHA256SUMS`, and create a draft GitHub Release.
3. Inspect the draft, then have the user publish it as a pre-release.
4. Download the published `SHA256SUMS` and update
   `scripts/release-pins.env` with the release version and all four digests.
   Land that pin commit through the normal pull request flow.
5. After the pin commit reaches the default branch, validate a managed install
   against a live Herdr on Linux:

   ```sh
   herdr plugin install mageyuki/herdr-top
   ```

   Confirm that the build command downloads and verifies the archive, that the
   installed `bin/herdr-top --version` reports the pinned version, and that the
   Herdr Top pane launches. Record the validation output with the release
   evidence.

The pins deliberately land after publication. They are the repository's trust
anchor for an already-published artifact, rather than checksums fetched from
the same location as the archive.

Until the first pin commit lands, `scripts/release-pins.env` has an empty
version and `herdr plugin install mageyuki/herdr-top` fails closed with `no
release pinned yet`. This is expected; do not bypass checksum pinning to make
an unpinned install succeed.

## Keep the tag and crate version aligned

The version without the leading `v` must exactly equal the package version in
`Cargo.toml`. For example, tag `v0.1.0` requires package version `0.1.0`. The
release workflow rejects a tag whose version differs, before publishing any
release.

## Enable Marketplace discovery after usability approval

Keep the repository's `herdr-plugin` GitHub topic unset while the release is
still being evaluated. After the user declares the release usable, the user
may promote the pre-release and add the `herdr-plugin` topic so Herdr
Marketplace can discover the plugin.
