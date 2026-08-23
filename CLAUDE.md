# herdr-top Agent Guide

herdr-top is a terminal monitor for herdr-managed Claude and Codex agent sessions.

## Commands

- `make build` builds the optimized release binary.
- `make test` runs all targets with all features, followed by doctests.
- `make lint` checks formatting and runs Clippy with warnings denied.
- `make fmt` formats the Rust sources.

## Source layout

- `src/main.rs` parses the CLI and coordinates monitor, doctor, hook, and emit commands.
- `src/lib.rs` exposes the library modules used by the binary and integration tests.
- `src/activity.rs` defines immutable operator activity and terminal-visibility read models.
- `src/doctor.rs` builds deterministic, non-mutating diagnostic reports and renderers.
- `src/hook_adapter.rs` validates and converts Claude and Codex hook payloads.
- `src/identity.rs` plans and applies task-run identity binding and merges.
- `src/lockfile.rs` manages per-session state roots, sentinels, and owner locks.
- `src/operator.rs` maintains the closed live operator projection.
- `src/performance.rs` records bounded runtime performance observations and degradation state.
- `src/reducer.rs` implements reducer state machines, ordinal allocation, and gap reconciliation.
- `src/rendezvous.rs` validates runtime rendezvous paths and Controller sockets.
- `src/session_key.rs` resolves session names and encodes path-safe session keys.
- `src/diagnostics/` contains shared diagnostic contracts plus local and remote probes.
- `src/herdr/` contains protocol types, wire transport, collection, and Controller event handling.
- `src/model/` defines domain entities, identifiers, states, and graph analysis.
- `src/provider/` discovers and tails provider files and adapts Claude and Codex records.
- `src/store/` owns SQLite schema, restoration, retention, and the dedicated writer.
- `src/tui/` implements application state, DAG projection, rendering, and key handling.

## Conventions

- Use conventional commit messages.
- Keep compatibility with the minimum supported Rust version, 1.97.1.
- Treat `docs/design/herdr-top-mvp.md` as the canonical design document.
- Put specifications and implementation plans under `docs/internal/superpowers/`.
- Follow `docs/guides/release-process.md` when preparing a release.
