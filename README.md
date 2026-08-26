# herdr-top

A terminal monitor for herdr-managed agent sessions.

herdr-top turns a running Herdr session into a live execution tree of
workspaces, tabs, panes, Task Runs, and native agent children. It combines
Herdr's event stream with the Claude Code and Codex provider session logs,
while keeping physical execution and semantic task dependencies distinct. Use
it to see where work is running, what each worker is doing, and which
observations are live, degraded, or disconnected. Quitting the monitor does
not stop the agents it observes.

Its sweet spot is agent orchestration. A single chat in a single pane is easy
to follow -- but once a session fans out into sub-agents and dispatched
background tasks, what is actually running becomes invisible. herdr-top keeps
every agent, sub-agent, and dispatched task in the session in one live view:
what is running, what is blocked, and what has finished. It is like top for your
agent swarm.

## 30-second quickstart

Install the Herdr plugin:

```sh
herdr plugin install mageyuki/herdr-top
```

The plugin route requires a published release pin. Open its `Herdr Top` pane in
the session to inspect the agent tree, including headless workers, with no hook
registration, `emit` wiring, or other configuration. The standalone installer
always works regardless of plugin-pin availability and provides the same
zero-configuration monitoring:

```sh
curl --fail --location --silent --show-error \
  https://raw.githubusercontent.com/mageyuki/herdr-top/main/install.sh | bash
```

Then run it from a pane inside the Herdr session you want to inspect:

```sh
herdr-top
```

## The TUI at a glance

The header reports the monitored session and observation health. The main
viewport follows the execution tree, the lower panel describes the selected
scope, and the footer keeps the primary controls visible.

```text
┌ Herdr Top ─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│host:devbox | session:default | up:00:17:42 | workspaces:2 | LIVE | lag:12ms | sources:herdr=available;ctl=avail…       │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Execution tree ────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Session: default                                                                                                      │
│  ├── Workspace: api                                                                                                    │
│  │   └── Tab: implementation                                                                                           │
│  │       ├── Pane: w1:p1 (controller)                                                                                  │
│  │       │   └── ● Claude controller — tool_use: Agent                                 fable-5  high  8400 8.2/s 17m03s│
│  │       │       └── ● Codex — running command                                     gpt-5.6-sol xhigh  2140  11/s 03m14s│
│  │       ├── Pane: w1:p2 (review)                                                                                      │
│  │       │   └── ⚠ Claude Review failures — tool_use: Bash                          sonnet-4-5  high   920 1.9/s 08m01s│
│  │       └── Pane: w1:p3 (tests)                                                                                       │
│  │           └── ✓ Codex Run unit tests                                            gpt-5.6-sol xhigh  1975  12/s 02m48s│
│  ├── Workspace: docs                                                                                                   │
│  │   └── Tab: review                                                                                                   │
│  │       └── Pane: w2:p1                                                                                               │
│  │           └── ◌ Claude Draft guide outline                                        haiku-4-5   low   310 7.4/s 00m42s│
│  └── Unattached Task Runs                                                                                              │
│      └── ◌ Codex orphan-session [unlinked]                                         gpt-5.6-sol  high     —     —      —│
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Activity for selected item ────────────────────────────────────────────────────────────────────────────────────────────┐
│p:healthy | ctl:available | D4:0                                                                                        │
│Selected: ● Codex — running command · 03m14s                                                                            │
│selection: stable                                                                                                       │
│Newest: at=1723456789012 kind=agent_activity source_type=activity provider=Codex durability=current_only                │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
q: stop Top only; agents continue | detach: Top runs | ↑↓ select | f/End follow | tab view | / filter | s summary | ? help
```

The example uses a 120-column tree inner width, so all five fixed metric
columns are visible: model, effort, output tokens, output tokens per second,
and time. The TUI paints no column-header row. As the tree pane narrows, it
drops model, effort, token rate, token total, and time in that order after
truncating labels and compressing deep indentation.

Lineage evidence has three positions: Claude Agent-tool `.meta.json` sidecars;
real Bash-command invocations of `codex exec resume <uuid>` or
`claude --resume <uuid>` plus leading `CLAUDE_CONFIG_DIR=` assignments; and
Codex's typed `sub_agent_activity.agent_thread_id` child references. UUIDs must
exactly match discovered artifacts. Quoted reports, printed lookalikes, bare
spawns, spawn output, tool-result bodies, and UUID-shaped text elsewhere are not
evidence. Without one of those positions, a child is not admitted or displayed;
herdr-top never guesses from timing, neighboring panes, or shared paths.

See the [TUI guide](docs/tui.md) for the complete key map, row grammar, views,
overlays, and visibility rules.

## Install

Release archives use these names:

| Platform | Release target | Archive pattern | Methods |
| --- | --- | --- | --- |
| macOS, Apple silicon | `aarch64-apple-darwin` | `herdr-top-<version>-aarch64-apple-darwin.tar.gz` | Installer, release archive, Herdr plugin |
| macOS, Intel | `x86_64-apple-darwin` | `herdr-top-<version>-x86_64-apple-darwin.tar.gz` | Installer, release archive, Herdr plugin |
| Linux, x86-64 | `x86_64-unknown-linux-gnu` | `herdr-top-<version>-x86_64-unknown-linux-gnu.tar.gz` | Installer, release archive, Herdr plugin |
| Linux, ARM64 | `aarch64-unknown-linux-gnu` | `herdr-top-<version>-aarch64-unknown-linux-gnu.tar.gz` | Installer, release archive, Herdr plugin |

The installer downloads `SHA256SUMS`, verifies the selected archive, and
installs `herdr-top` into `~/.local/bin`. Set `INSTALL_DIR` to choose another
destination, or set `HERDR_TOP_VERSION=0.1.0` to pin a release instead of using
the latest one.

For a piped install with a custom destination, put `INSTALL_DIR` on the Bash
side:

```sh
curl --fail --location --silent --show-error https://raw.githubusercontent.com/mageyuki/herdr-top/main/install.sh | INSTALL_DIR=/some/bin bash
```

The Linux artifacts are `unknown-linux-gnu` builds, not musl builds. They
require glibc and are built on Ubuntu 24.04 runners; compatibility with systems
that provide an older glibc is not established by the release workflow.

To install through Herdr instead, use:

```sh
herdr plugin install mageyuki/herdr-top
```

The plugin route becomes available once the release pin commit lands after each
release.

The included plugin manifest declares plugin ID `mageyuki.herdr-top`, version
`0.1.0`, Herdr `0.8.0` as the minimum, and Linux and macOS as its platforms. Its
build command is `scripts/fetch-release.sh`; its `top` pane is a tab titled
`Herdr Top` that runs `bin/herdr-top`.

## Documentation

- [TUI guide](docs/tui.md)
- [CLI reference](docs/cli.md)
- [Optional Controller event precision layer](docs/guides/controller-emit-setup.md)
  -- sharpens the zero-configuration log view with explicit lifecycle
  transitions, Controller-authored subjects, dispatch edges that do not depend
  on session-ID evidence, and explicit dependency edges.
- [Release process](docs/guides/release-process.md)
- [MVP design](docs/design/herdr-top-mvp.md)
- [Design records (ADRs)](docs/adr/)

## Contributing

The minimum supported Rust version is 1.97.1. Use the repository's standard
targets while developing:

```sh
make build
make test
make lint
make fmt
```

Use conventional commits, keep changes focused, and include tests for behavior
changes.

## License

herdr-top is available under the [MIT License](LICENSE).
