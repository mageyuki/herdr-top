# herdr-top

A terminal monitor for herdr-managed agent sessions.

herdr-top turns a running Herdr session into a live execution tree of
workspaces, tabs, panes, Task Runs, and native agent children. It combines
Herdr's event stream with Claude Code and Codex metadata and hook events, while
keeping physical execution and semantic task dependencies distinct. Use it to
see where work is running, what each worker is doing, and which observations
are live, degraded, or disconnected. Quitting the monitor does not stop the
agents it observes.

## 30-second quickstart

Install the latest release into `~/.local/bin`:

```sh
curl --fail --location --silent --show-error \
  https://raw.githubusercontent.com/mageyuki/herdr-top/main/install.sh | bash
```

For scripted or automated use, download the installer first so a failed
download is detectable:

```sh
curl --fail --location --silent --show-error https://raw.githubusercontent.com/mageyuki/herdr-top/main/install.sh -o /tmp/herdr-top-install.sh && bash /tmp/herdr-top-install.sh
```

Then, from a pane inside the Herdr session you want to inspect:

```sh
herdr-top
```

## The TUI at a glance

The header reports the monitored session and observation health. The main
viewport follows the execution tree, the lower panel describes the selected
scope, and the footer keeps the primary controls visible.

```text
┌ Herdr Top ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│host:devbox | session:default | up:00:17:42 | workspaces:2 | LIVE | lag:12ms | sources:herdr=available;controller=availa…         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Execution tree ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Session: default                                                                                                                │
│  ├── Workspace: api                                                                                                              │
│  │   └── Tab: implementation                                                                                                     │
│  │       ├── Pane: w1:p1                                                                                                         │
│  │       │   └── claude-code Improve API timeout handling — tool_use: Bash [model:claude-sonnet] [running] · 17m03s              │
│  │       │       └── Claude native agent: investigate [state:working] [model:claude-sonnet] [last:1723456789012ms]               │
│  │       └── Pane: w1:p2                                                                                                         │
│> │           └── Codex tests [model:gpt-5-codex] [blocked] · 03m14s [dispatched by: controller]                                  │
│  └── Workspace: docs                                                                                                             │
│      └── Tab: review                                                                                                             │
│          └── Pane: w2:p1                                                                                                         │
│              └── Codex docs [running] · 00m42s [unlinked]                                                                        │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Activity for selected item ──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│p:healthy | ctl:available | D4:0                                                                                                  │
│Selected: Codex tests [model:gpt-5-codex] [blocked] · 03m14s [dispatched by: controller]                                          │
│selection: stable                                                                                                                 │
│Newest: at=1723456789012 kind=controller_event source_type=blocked provider=Codex durability=durable                              │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
q: stop Top only; agents continue | detach: Top runs | ↑↓ select | f/End follow | tab view | / filter | s summary | ? help | c clear
```

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
- [Controller event hook setup](docs/guides/controller-emit-setup.md)
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
